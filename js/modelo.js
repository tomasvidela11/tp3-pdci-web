// =============================================================================================
// modelo.js — Lógica de la Host Application de Sparkplug B (frente B, bloques WB3 y WB4)
//
// Mantiene el último valor y el estado (en línea / fuera de línea) de cada Edge Node, de cada
// dispositivo y de cada métrica del grupo, como pide la consigna («Host Application»), y manda los
// comandos. No toca la pantalla: la interfaz (interfaz.js) lee este modelo.
//
// Reglas de Sparkplug 3.0.0 que se aplican (cap. 5 salvo indicación):
//   - NBIRTH: abre la sesión del nodo; guarda bdSeq, el datatype de cada métrica y seq.
//   - NDEATH: si su bdSeq es el del NBIRTH vigente, el nodo pasa a fuera de línea con la hora de
//     la web, y todas sus métricas y sus dispositivos a STALE / fuera de línea
//     (tck-id-operational-behavior-edge-node-termination-host-action-ndeath-*). Si el bdSeq no
//     coincide (NDEATH de una conexión anterior que llegó tarde) o el nodo ya estaba fuera de
//     línea (el segundo NDEATH del cierre intencional, D32), se ignora.
//   - DDEATH: el dispositivo pasa a fuera de línea y sus métricas a STALE con el timestamp del
//     DDEATH (tck-id-operational-behavior-edge-node-termination-host-action-ddeath-*).
//   - seq: NBIRTH, NDATA, DBIRTH, DDATA y DDEATH llevan seq de 0 a 255, uno más en cada mensaje
//     del nodo. Ante un salto se arranca el Reorder Timeout; si los mensajes que faltan no llegan,
//     se pide Rebirth por NCMD (tck-id-operational-behavior-host-reordering-*).
//   - Datos sin BIRTH previo, o una métrica que no estaba en el BIRTH → Rebirth (cap. 5,
//     «Sparkplug Host Applications»; lo pide la consigna).
//   - Si la web pierde la conexión con el broker, todas las métricas pasan a STALE.
//   - Comandos (WB4): NCMD y DCMD con QoS 0, sin retain, con timestamp y sin seq; las métricas sin
//     datatype y con el valor en el campo del datatype del BIRTH (carga.js, WB1.5).
//
// Bloques:
//   WB3.1 Despacho por tipo de mensaje   WB3.2 NBIRTH        WB3.3 DBIRTH
//   WB3.4 NDATA y DDATA                  WB3.5 NDEATH        WB3.6 DDEATH
//   WB3.7 seq y Reorder Timeout          WB3.8 Datos sin BIRTH y métricas desconocidas → Rebirth
//   WB3.9 Conexión con el broker perdida
//   WB4.1 DCMD (validación)              WB4.2 Límite de tasa (4 por segundo, último valor)
//   WB4.3 NCMD Rebirth                   WB4.4 Confirmación por DDATA
// =============================================================================================

import { CALIDAD, NOMBRE_DE_TIPO, TIPOS, calidadDe, codificarComando, decodificar, motivoFueraDeTipo, valorDe } from "./carga.js";

const CON_SEQ = new Set(["NBIRTH", "NDATA", "DBIRTH", "DDATA", "DDEATH"]);
export const ESTADO = { DESCONOCIDO: "desconocido", EN_LINEA: "en línea", FUERA: "fuera de línea" };

/**
 * @typedef {object} Metrica
 * @property {string} nombre
 * @property {number} datatype
 * @property {*} valor
 * @property {number|null} marca      timestamp de la métrica (ms)
 * @property {number} calidad         0 BAD, 192 GOOD, 500 STALE
 * @property {object} propiedades     las del BIRTH (Escribible, Minimo, Maximo…)
 * @property {number} recibida        hora de la web en que llegó el último valor (ms)
 */

export class HostSparkplug {
    /**
     * @param {object} config
     * @param {string} config.grupo              group_id
     * @param {number} [config.reorderMs=2000]   Reorder Timeout (tck-id-operational-behavior-host-reordering-param)
     * @param {number} [config.esperaRebirthMs=5000]  no se repite un Rebirth antes de este tiempo
     * @param {number} [config.periodoComandoMs=250]  WB4.2: a lo sumo un DCMD cada 250 ms (4 por segundo)
     * @param {(topico: string, carga: Uint8Array) => boolean} publicar   manda un NCMD/DCMD
     * @param {(tipo: string, texto: string) => void} alEvento          registro de eventos
     * @param {() => void} alCambio                                     algo cambió (redibujar)
     */
    constructor(config, publicar, alEvento, alCambio) {
        this.config = { reorderMs: 2000, esperaRebirthMs: 5000, periodoComandoMs: 250, ...config };
        this.publicar = publicar;
        this.alEvento = alEvento;
        this.alCambio = alCambio;
        this.nodos = new Map();            // edge_node_id → nodo
        this.contadores = { mensajes: 0, rebirthsPedidos: 0, saltosSeq: 0, malformados: 0 };
        this.pendientesDcmd = new Map();   // "nodo/disp" → Map(nombre → valor) por mandar (WB4.2)
        this.ultimoDcmd = 0;
        this.temporizadorDcmd = null;
        this.confirmaciones = [];          // DCMD mandados que esperan su DDATA (WB4.4)
    }

    _nodo(id) {
        if (!this.nodos.has(id)) {
            this.nodos.set(id, {
                id, estado: ESTADO.DESCONOCIDO, bdSeq: null, seqEsperado: null, faltantes: new Set(),
                temporizadorReorden: null, metricas: new Map(), dispositivos: new Map(),
                desde: null, ultimoMensaje: null, ultimoRebirthPedido: 0, rebirthsPedidos: 0,
                saltosSeq: 0, ultimoSeq: null,
            });
        }
        return this.nodos.get(id);
    }

    _dispositivo(nodo, id) {
        if (!nodo.dispositivos.has(id)) {
            nodo.dispositivos.set(id, { id, estado: ESTADO.DESCONOCIDO, metricas: new Map(), desde: null });
        }
        return nodo.dispositivos.get(id);
    }

    // -----------------------------------------------------------------------------------------
    // WB3.1 Despacho por tipo de mensaje
    // -----------------------------------------------------------------------------------------

    /**
     * Procesa un mensaje del grupo (spBv1.0/<grupo>/<tipo>/<nodo>[/<dispositivo>]).
     * @param {string} topico
     * @param {Uint8Array} carga
     */
    procesar(topico, carga) {
        const partes = topico.split("/");
        if (partes.length < 4 || partes[0] !== "spBv1.0" || partes[1] !== this.config.grupo) return;
        const [, , tipo, nodoId, dispId] = partes;
        this.contadores.mensajes += 1;
        let p;
        try {
            p = decodificar(carga);
        } catch (e) {
            this.contadores.malformados += 1;
            this.alEvento("error", `${tipo} de ${nodoId}: payload mal formado (${e.message})`);
            return;
        }
        const nodo = this._nodo(nodoId);
        nodo.ultimoMensaje = Date.now();
        if (tipo === "NCMD" || tipo === "DCMD") {
            const nombres = p.metricas.map(m => m.nombre).join(", ");
            this.alEvento("cmd", `${tipo} en el broker → ${nodoId}${dispId ? "/" + dispId : ""}: ${nombres}`);
            return;
        }
        if (tipo === "NDEATH") return this._ndeath(nodo, p);
        if (CON_SEQ.has(tipo) && tipo !== "NBIRTH") {
            // WB3.8: sin NBIRTH de esta sesión no se conocen los datatypes: se pide Rebirth.
            if (nodo.estado !== ESTADO.EN_LINEA) {
                this.pedirRebirth(nodo.id, `${tipo} sin NBIRTH previo`);
                return;
            }
            this._controlarSeq(nodo, p.seq, tipo);
        }
        switch (tipo) {
            case "NBIRTH": this._nbirth(nodo, p); break;
            case "DBIRTH": this._dbirth(nodo, this._dispositivo(nodo, dispId), p); break;
            case "NDATA": this._datos(nodo, nodo.metricas, p, "NDATA"); break;
            case "DDATA": this._ddata(nodo, dispId, p); break;
            case "DDEATH": this._ddeath(nodo, this._dispositivo(nodo, dispId), p); break;
            default: return;
        }
        this.alCambio();
    }

    // -----------------------------------------------------------------------------------------
    // WB3.2 NBIRTH
    // -----------------------------------------------------------------------------------------

    _nbirth(nodo, p) {
        const bdSeq = p.metricas.find(m => m.nombre === "bdSeq");
        let nuevas;
        try {
            nuevas = this._metricasDeBirth(p);
        } catch (e) {
            this.contadores.malformados += 1;
            this.alEvento("error", `NBIRTH de ${nodo.id} con una métrica inválida (${e.message})`);
            return;
        }
        nodo.metricas = nuevas;
        nodo.bdSeq = bdSeq ? valorDe(bdSeq, bdSeq.datatype || TIPOS.Int64) : null;
        nodo.estado = ESTADO.EN_LINEA;
        nodo.desde = p.timestamp;
        nodo.seqEsperado = (p.seq + 1) % 256;
        nodo.ultimoSeq = p.seq;
        nodo.ultimoRebirthPedido = 0;         // llegó el NBIRTH: un problema nuevo pide Rebirth enseguida
        this._cancelarReorden(nodo);
        // Un NBIRTH abre una sesión nueva: los dispositivos valen recién con su DBIRTH.
        for (const d of nodo.dispositivos.values()) {
            d.estado = ESTADO.DESCONOCIDO;
            for (const m of d.metricas.values()) m.calidad = CALIDAD.STALE;
        }
        this.alEvento("birth", `NBIRTH de ${nodo.id}: bdSeq ${nodo.bdSeq}, seq ${p.seq}, ${nuevas.size} métricas`);
    }

    /** Métricas de un BIRTH: el datatype es obligatorio ahí (tck-id-payloads-nbirth-*, -dbirth-*). */
    _metricasDeBirth(p) {
        const ahora = Date.now();
        const salida = new Map();
        for (const m of p.metricas) {
            if (m.datatype === null) throw new Error(`${m.nombre} sin datatype`);
            salida.set(m.nombre, {
                nombre: m.nombre, datatype: m.datatype, valor: valorDe(m, m.datatype), marca: m.timestamp,
                calidad: calidadDe(m), propiedades: m.propiedades, recibida: ahora,
            });
        }
        return salida;
    }

    // -----------------------------------------------------------------------------------------
    // WB3.3 DBIRTH
    // -----------------------------------------------------------------------------------------

    _dbirth(nodo, disp, p) {
        try {
            disp.metricas = this._metricasDeBirth(p);
        } catch (e) {
            this.contadores.malformados += 1;
            this.alEvento("error", `DBIRTH de ${disp.id} con una métrica inválida (${e.message})`);
            return;
        }
        disp.estado = ESTADO.EN_LINEA;
        disp.desde = p.timestamp;
        this.alEvento("birth", `DBIRTH de ${nodo.id}/${disp.id}: seq ${p.seq}, ${disp.metricas.size} métricas`);
        this._revisarConfirmaciones(nodo.id, disp.id, disp.metricas);
    }

    // -----------------------------------------------------------------------------------------
    // WB3.4 NDATA y DDATA
    // -----------------------------------------------------------------------------------------

    _ddata(nodo, dispId, p) {
        const disp = nodo.dispositivos.get(dispId);
        if (!disp || disp.estado !== ESTADO.EN_LINEA) {
            this.pedirRebirth(nodo.id, `DDATA de ${dispId} sin DBIRTH previo`);
            return;
        }
        if (this._datos(nodo, disp.metricas, p, `DDATA de ${dispId}`)) {
            this._revisarConfirmaciones(nodo.id, dispId, disp.metricas);
        }
    }

    /** Aplica los valores de un DATA con los datatypes del BIRTH. Devuelve false si pidió Rebirth. */
    _datos(nodo, metricas, p, que) {
        const ahora = Date.now();
        for (const m of p.metricas) {
            const actual = metricas.get(m.nombre);
            if (!actual) {
                // WB3.8: métrica que no estaba en el BIRTH.
                this.pedirRebirth(nodo.id, `${que} con la métrica «${m.nombre}», que no estaba en el BIRTH`);
                return false;
            }
            // Un valor más viejo que el que ya se tiene (DATA atrasado) no lo pisa.
            if (m.timestamp !== null && actual.marca !== null && m.timestamp < actual.marca) continue;
            try {
                actual.valor = valorDe(m, actual.datatype);
            } catch (e) {
                this.contadores.malformados += 1;
                this.alEvento("error", `${que}: ${m.nombre} ${e.message}`);
                continue;
            }
            actual.marca = m.timestamp;
            actual.calidad = calidadDe(m);
            actual.recibida = ahora;
        }
        return true;
    }

    // -----------------------------------------------------------------------------------------
    // WB3.5 NDEATH
    // -----------------------------------------------------------------------------------------

    _ndeath(nodo, p) {
        const m = p.metricas.find(x => x.nombre === "bdSeq");
        let bdSeq = null;
        try {
            bdSeq = m ? valorDe(m, m.datatype || TIPOS.Int64) : null;
        } catch {
            bdSeq = null;
        }
        // En línea: vale solo con el bdSeq del NBIRTH vigente. Fuera de línea: es repetido (D32) o
        // viejo. Desconocido (la web recién abre o perdió el broker y no sabe qué sesión está
        // vigente): se acepta; si era viejo, el próximo DATA llega «sin NBIRTH» y se pide Rebirth.
        if (nodo.estado === ESTADO.FUERA || (nodo.estado === ESTADO.EN_LINEA && bdSeq !== nodo.bdSeq)) {
            const motivo = nodo.estado === ESTADO.FUERA && bdSeq === nodo.bdSeq
                ? "repetido (el nodo ya estaba fuera de línea)"
                : `de otra conexión (bdSeq ${bdSeq}; el NBIRTH vigente tiene ${nodo.bdSeq})`;
            this.alEvento("info", `NDEATH de ${nodo.id} ${motivo}: se ignora`);
            return;
        }
        nodo.bdSeq = bdSeq;
        const ahora = Date.now();             // el NDEATH puede venir del broker: hora de la web
        nodo.estado = ESTADO.FUERA;
        nodo.desde = ahora;
        this._cancelarReorden(nodo);
        for (const x of nodo.metricas.values()) { x.calidad = CALIDAD.STALE; x.recibida = ahora; }
        for (const d of nodo.dispositivos.values()) {
            d.estado = ESTADO.FUERA;
            d.desde = ahora;
            for (const x of d.metricas.values()) { x.calidad = CALIDAD.STALE; x.recibida = ahora; }
        }
        this.alEvento("death", `NDEATH de ${nodo.id} (bdSeq ${bdSeq}): nodo fuera de línea; sus métricas y dispositivos en STALE`);
        this.alCambio();
    }

    // -----------------------------------------------------------------------------------------
    // WB3.6 DDEATH
    // -----------------------------------------------------------------------------------------

    _ddeath(nodo, disp, p) {
        disp.estado = ESTADO.FUERA;
        disp.desde = p.timestamp;             // lo publica el Edge Node: su timestamp vale
        for (const x of disp.metricas.values()) { x.calidad = CALIDAD.STALE; x.recibida = Date.now(); }
        this.alEvento("death", `DDEATH de ${nodo.id}/${disp.id} (seq ${p.seq}): dispositivo fuera de línea; métricas en STALE`);
    }

    // -----------------------------------------------------------------------------------------
    // WB3.7 seq y Reorder Timeout
    // -----------------------------------------------------------------------------------------

    _controlarSeq(nodo, seq, tipo) {
        nodo.ultimoSeq = seq;
        if (seq === null || seq === undefined) {
            this.alEvento("error", `${tipo} de ${nodo.id} sin seq`);
            return;
        }
        if (seq === nodo.seqEsperado) {
            nodo.seqEsperado = (seq + 1) % 256;
            return;
        }
        if (nodo.faltantes.has(seq)) {
            // Llegó tarde uno de los que faltaban (tck-id-operational-behavior-host-reordering-success).
            nodo.faltantes.delete(seq);
            if (nodo.faltantes.size === 0) {
                this._cancelarReorden(nodo);
                this.alEvento("info", `seq de ${nodo.id}: llegaron los mensajes que faltaban; sigue normal`);
            }
            return;
        }
        // Salto: se anotan los que faltan y se arranca el Reorder Timeout.
        for (let s = nodo.seqEsperado; s !== seq && nodo.faltantes.size < 255; s = (s + 1) % 256) {
            nodo.faltantes.add(s);
        }
        nodo.saltosSeq += 1;
        this.contadores.saltosSeq += 1;
        this.alEvento("seq", `Salto de seq en ${nodo.id}: se esperaba ${nodo.seqEsperado} y llegó ${seq} (${tipo}); faltan ${nodo.faltantes.size}. Reorder Timeout de ${this.config.reorderMs} ms`);
        nodo.seqEsperado = (seq + 1) % 256;
        if (!nodo.temporizadorReorden) this._armarReorden(nodo, this.config.reorderMs);
    }

    _armarReorden(nodo, espera) {
        nodo.temporizadorReorden = setTimeout(() => {
            nodo.temporizadorReorden = null;
            if (nodo.faltantes.size === 0) return;
            const n = nodo.faltantes.size;
            if (this.pedirRebirth(nodo.id, `faltan ${n} mensajes (seq) tras el Reorder Timeout`)) {
                nodo.faltantes.clear();
            } else {
                // No salió (hay otro Rebirth en curso o no hay conexión): el hueco no se olvida.
                this._armarReorden(nodo, 1000);
            }
        }, espera);
    }

    _cancelarReorden(nodo) {
        if (nodo.temporizadorReorden) clearTimeout(nodo.temporizadorReorden);
        nodo.temporizadorReorden = null;
        nodo.faltantes.clear();
    }

    // -----------------------------------------------------------------------------------------
    // WB3.9 Conexión con el broker perdida
    // -----------------------------------------------------------------------------------------

    /**
     * La web dejó de recibir: todo pasa a STALE y el estado de cada nodo a «desconocido». Al volver,
     * el primer DATA de cada nodo llega «sin NBIRTH previo» y se pide Rebirth (WB3.8).
     */
    conexionPerdida() {
        const ahora = Date.now();
        for (const nodo of this.nodos.values()) {
            this._cancelarReorden(nodo);
            nodo.estado = ESTADO.DESCONOCIDO;
            for (const x of nodo.metricas.values()) { x.calidad = CALIDAD.STALE; x.recibida = ahora; }
            for (const d of nodo.dispositivos.values()) {
                d.estado = ESTADO.DESCONOCIDO;
                for (const x of d.metricas.values()) { x.calidad = CALIDAD.STALE; x.recibida = ahora; }
            }
        }
        this.pendientesDcmd.clear();
        this.alCambio();
    }

    // -----------------------------------------------------------------------------------------
    // WB4.1 DCMD (validación)
    // -----------------------------------------------------------------------------------------

    /**
     * Revisa si se puede mandar un valor a una métrica del dispositivo. Devuelve el motivo del
     * rechazo, o null si se puede.
     */
    motivoRechazo(nodoId, dispId, nombre, valor) {
        const nodo = this.nodos.get(nodoId);
        if (!nodo || nodo.estado !== ESTADO.EN_LINEA) return "el Edge Node no está en línea";
        const disp = nodo.dispositivos.get(dispId);
        if (!disp || disp.estado !== ESTADO.EN_LINEA) return "el dispositivo no está en línea";
        const m = disp.metricas.get(nombre);
        if (!m) return `${nombre} no está en el DBIRTH`;
        if (m.propiedades.Escribible === false) return `${nombre} no es escribible`;
        const fuera = motivoFueraDeTipo(valor, m.datatype);
        if (fuera) return fuera;
        const { Minimo: min, Maximo: max } = m.propiedades;
        if (typeof min === "number" && valor < min) return `${nombre} va de ${min} a ${max}`;
        if (typeof max === "number" && valor > max) return `${nombre} va de ${min} a ${max}`;
        return null;
    }

    /**
     * Pide mandar un valor por DCMD. Respeta el límite de tasa (WB4.2): si hace menos de 250 ms
     * que salió el último, se guarda y sale en el próximo turno con el valor más reciente.
     * @returns {string|null} motivo del rechazo, o null si quedó enviado o programado
     */
    escribir(nodoId, dispId, nombre, valor) {
        const motivo = this.motivoRechazo(nodoId, dispId, nombre, valor);
        if (motivo) {
            this.alEvento("error", `DCMD ${nombre} = ${valor} no enviado: ${motivo}`);
            return motivo;
        }
        const clave = `${nodoId}/${dispId}`;
        if (!this.pendientesDcmd.has(clave)) this.pendientesDcmd.set(clave, new Map());
        this.pendientesDcmd.get(clave).set(nombre, valor);
        this._programarDcmd();
        return null;
    }

    // -----------------------------------------------------------------------------------------
    // WB4.2 Límite de tasa: a lo sumo un DCMD cada 250 ms, con los últimos valores pedidos
    // -----------------------------------------------------------------------------------------

    _programarDcmd() {
        if (this.temporizadorDcmd) return;
        const espera = Math.max(0, this.ultimoDcmd + this.config.periodoComandoMs - Date.now());
        this.temporizadorDcmd = setTimeout(() => {
            this.temporizadorDcmd = null;
            this._mandarPendientes();
        }, espera);
    }

    _mandarPendientes() {
        const [clave, valores] = this.pendientesDcmd.entries().next().value || [];
        if (!clave) return;
        this.pendientesDcmd.delete(clave);
        const [nodoId, dispId] = clave.split("/");
        const metricas = [];
        for (const [nombre, valor] of valores) {
            // Se vuelve a validar: entre el pedido y el envío pudo llegar un DDEATH.
            const motivo = this.motivoRechazo(nodoId, dispId, nombre, valor);
            if (motivo) {
                this.alEvento("error", `DCMD ${nombre} = ${valor} no enviado: ${motivo}`);
                continue;
            }
            const m = this.nodos.get(nodoId).dispositivos.get(dispId).metricas.get(nombre);
            metricas.push({ nombre, datatype: m.datatype, valor });
        }
        if (metricas.length > 0) {
            const ahora = Date.now();
            const carga = codificarComando(metricas, ahora);
            const topico = `spBv1.0/${this.config.grupo}/DCMD/${nodoId}/${dispId}`;
            if (this.publicar(topico, carga)) {
                this.ultimoDcmd = ahora;
                const texto = metricas.map(m => `${m.nombre} = ${m.valor}`).join(", ");
                this.alEvento("cmd", `DCMD enviado a ${nodoId}/${dispId}: ${texto}`);
                for (const m of metricas) {
                    // Un valor nuevo reemplaza al pedido anterior de la misma métrica (no es un error).
                    this.confirmaciones = this.confirmaciones.filter(c => !(c.nodoId === nodoId && c.dispId === dispId && c.nombre === m.nombre));
                    this.confirmaciones.push({ nodoId, dispId, nombre: m.nombre, valor: m.valor, enviado: ahora });
                }
            } else {
                this.alEvento("error", "DCMD no enviado: la sesión con el broker no está lista");
            }
        }
        if (this.pendientesDcmd.size > 0) this._programarDcmd();
        this.alCambio();
    }

    // -----------------------------------------------------------------------------------------
    // WB4.3 NCMD Rebirth
    // -----------------------------------------------------------------------------------------

    /**
     * Pide Rebirth al nodo (NCMD con «Node Control/Rebirth» = true). No se repite antes de
     * esperaRebirthMs, para no inundar al nodo si los datos siguen llegando mientras tanto.
     * @param {string} nodoId
     * @param {string} motivo
     * @param {boolean} [aMano=false]  pedido por el usuario: se manda igual
     * @returns {boolean} true si se mandó
     */
    pedirRebirth(nodoId, motivo, aMano = false) {
        const nodo = this._nodo(nodoId);
        const ahora = Date.now();
        if (!aMano && ahora - nodo.ultimoRebirthPedido < this.config.esperaRebirthMs) return false;
        const carga = codificarComando([{ nombre: "Node Control/Rebirth", datatype: TIPOS.Boolean, valor: true }], ahora);
        if (!this.publicar(`spBv1.0/${this.config.grupo}/NCMD/${nodoId}`, carga)) {
            if (aMano) this.alEvento("error", `Rebirth a ${nodoId} no enviado: la sesión con el broker no está lista`);
            return false;
        }
        nodo.ultimoRebirthPedido = ahora;
        nodo.rebirthsPedidos += 1;
        this.contadores.rebirthsPedidos += 1;
        this.alEvento("rebirth", `NCMD Rebirth a ${nodoId}: ${motivo}`);
        this.alCambio();
        return true;
    }

    // -----------------------------------------------------------------------------------------
    // WB4.4 Confirmación: el comando vale cuando el DDATA (o DBIRTH) trae el valor pedido
    // -----------------------------------------------------------------------------------------

    _revisarConfirmaciones(nodoId, dispId, metricas) {
        const ahora = Date.now();
        this.confirmaciones = this.confirmaciones.filter(c => {
            if (c.nodoId !== nodoId || c.dispId !== dispId) return true;
            const m = metricas.get(c.nombre);
            // Vale solo un valor recibido después de mandar el comando.
            if (m && m.valor === c.valor && m.calidad === CALIDAD.GOOD && m.recibida >= c.enviado) {
                this.alEvento("ok", `${c.nombre} = ${c.valor} confirmado por el Edge Node en ${ahora - c.enviado} ms`);
                return false;
            }
            if (ahora - c.enviado > 5000) {
                this.alEvento("error", `${c.nombre} = ${c.valor} sin confirmar después de 5 s`);
                return false;
            }
            return true;
        });
    }

    /** Da por vencidos los comandos que esperan su DDATA hace más de 5 s (se llama cada segundo). */
    vencer() {
        const antes = this.confirmaciones.length;
        const ahora = Date.now();
        this.confirmaciones = this.confirmaciones.filter(c => {
            if (ahora - c.enviado <= 5000) return true;
            this.alEvento("error", `${c.nombre} = ${c.valor} sin confirmar después de 5 s`);
            return false;
        });
        if (this.confirmaciones.length !== antes) this.alCambio();
    }

    /**
     * Copia simple del modelo (para las pruebas y para depurar).
     * @returns {object}
     */
    instantanea() {
        const metricas = mapa => Object.fromEntries([...mapa].map(([k, m]) => [k, {
            valor: m.valor, datatype: NOMBRE_DE_TIPO[m.datatype], calidad: m.calidad, marca: m.marca,
            propiedades: m.propiedades,
        }]));
        return {
            contadores: { ...this.contadores },
            nodos: Object.fromEntries([...this.nodos].map(([id, n]) => [id, {
                estado: n.estado, bdSeq: n.bdSeq, ultimoSeq: n.ultimoSeq, seqEsperado: n.seqEsperado,
                rebirthsPedidos: n.rebirthsPedidos, saltosSeq: n.saltosSeq, metricas: metricas(n.metricas),
                dispositivos: Object.fromEntries([...n.dispositivos].map(([d, x]) => [d, {
                    estado: x.estado, metricas: metricas(x.metricas),
                }])),
            }])),
        };
    }
}
