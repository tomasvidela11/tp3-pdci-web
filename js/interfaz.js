// =============================================================================================
// interfaz.js — Pantalla de la web (frente B, bloque WB5)
//
// Dibuja el modelo de la Host Application (modelo.js) y pasa las órdenes del usuario. No conoce
// MQTT ni protobuf. El panel del dispositivo se arma con las métricas del DBIRTH (nombre,
// datatype y propiedades Escribible, Minimo, Maximo y ModbusRef): un esclavo con otras entradas y
// salidas se ve sin cambiar el código.
//
// Bloques:
//   WB5.1 Formulario de conexión            WB5.2 Cadena por tramos y diagnóstico de la falla
//   WB5.3 Dispositivo: entradas y salidas   WB5.4 Edge Node: estado de cada capa
//   WB5.5 Registro de eventos               WB5.6 Redibujo (agrupado) y reloj de 1 s
// =============================================================================================

import { CALIDAD, NOMBRE_DE_TIPO, TIPOS } from "./carga.js";
import { ESTADO } from "./modelo.js";

const $ = (id) => document.getElementById(id);
const MAX_EVENTOS = 300;

// Métricas del nodo que se muestran en cada capa (consigna, tabla 1, y las que agregan el contrato v1.0
// y el Edge Node: docs/CONTRATO_OPCUA.md, secciones 4 y 7). Un tercer elemento "ancha" pone la fila en
// todo el ancho de la capa, con el nombre arriba y el valor abajo (textos largos como Configuración).
const CAPAS = {
    "capa-modbus": [["Estado", "Status/MODBUS/Estado"], ["OK", "Status/MODBUS/OK"],
        ["CRC_ERROR", "Status/MODBUS/CRC_ERROR"], ["TIMEOUT", "Status/MODBUS/TIMEOUT"],
        ["TRAMA_INVALIDA", "Status/MODBUS/TRAMA_INVALIDA"], ["EXCEPCION", "Status/MODBUS/EXCEPCION"],
        ["Esclavo en línea", "Status/MODBUS/EsclavoEnLinea"], ["Configuración", "Status/MODBUS/Configuracion", "ancha"]],
    "capa-opcua": [["Estado", "Status/OPCUA/Estado"], ["Reconexiones", "Status/OPCUA/Reconexiones"]],
    "capa-mqtt": [["Reconexiones", "Status/MQTT/Reconexiones"], ["bdSeq", "bdSeq"]],
    "capa-sparkplug": [["Rebirths del nodo", "Status/Sparkplug/Rebirths"]],
};

function hora(ms) {
    const d = new Date(ms);
    return d.toLocaleTimeString("es-AR", { hour12: false }) + "." + String(d.getMilliseconds()).padStart(3, "0");
}

function hace(ms) {
    if (!ms) return "—";
    const s = Math.max(0, Math.round((Date.now() - ms) / 1000));
    return s < 60 ? `${s} s` : s < 3600 ? `${Math.floor(s / 60)} min` : `${Math.floor(s / 3600)} h`;
}

function texto(valor, datatype) {
    if (valor === null || valor === undefined) return "—";
    if (datatype === TIPOS.Boolean) return valor ? "true" : "false";
    return String(valor);
}

function nombreCalidad(c) {
    return c === CALIDAD.GOOD ? "GOOD" : c === CALIDAD.STALE ? "STALE" : c === CALIDAD.BAD ? "BAD" : String(c);
}

export class Interfaz {
    /**
     * @param {object} config  {url, usuario, nodo, dispositivo}
     * @param {object} acciones
     * @param {(url: string, usuario: string, clave: string) => void} acciones.conectar
     * @param {() => void} acciones.desconectar
     * @param {(nombre: string, valor: *) => void} acciones.escribir
     * @param {() => void} acciones.rebirth
     */
    constructor(config, acciones) {
        this.config = config;
        this.acciones = acciones;
        this.host = null;
        this.estadoSesion = { estado: "inicial", detalle: "" };
        this.tarjetas = new Map();          // nombre de métrica → elementos de su tarjeta (WB5.3)
        this.firmaDispositivo = "";
        this.filasNodo = new Map();
        this.ultimoUso = new Map();         // métrica → hora del último movimiento del usuario
        this.pendienteDibujo = false;
        this._formulario();
        setInterval(() => this.dibujar(), 1000);   // WB5.6: «hace X s» y comandos vencidos
    }

    // -----------------------------------------------------------------------------------------
    // WB5.1 Formulario de conexión
    // -----------------------------------------------------------------------------------------

    _formulario() {
        $("campo-url").value = this.config.url;
        $("campo-usuario").value = this.config.usuario;
        $("formulario").addEventListener("submit", (ev) => {
            ev.preventDefault();
            const clave = $("campo-clave").value;
            $("campo-clave").value = "";
            this.acciones.conectar($("campo-url").value.trim(), $("campo-usuario").value.trim(), clave);
        });
        $("boton-desconectar").addEventListener("click", () => this.acciones.desconectar());
        $("boton-rebirth").addEventListener("click", () => this.acciones.rebirth());
    }

    /** La sesión MQTT cambió de estado (WB2). */
    sesion(estado, detalle) {
        this.estadoSesion = { estado, detalle };
        const activa = estado === "conectando" || estado === "conectado" || estado === "desconectado";
        $("boton-conectar").disabled = activa;
        $("boton-desconectar").disabled = !activa;
        for (const id of ["campo-url", "campo-usuario", "campo-clave"]) $(id).disabled = activa;
        const p = $("pastilla-broker");
        const textos = {
            inicial: ["desconocido", "Sin conectar"], conectando: ["aviso", "Conectando…"],
            conectado: ["ok", "Conectada al broker"], desconectado: ["falla", "Sin conexión con el broker"],
            rechazado: ["falla", `Rechazada: ${detalle}`], cerrado: ["desconocido", "Desconectada"],
        };
        const [clase, t] = textos[estado] || textos.inicial;
        p.className = `pastilla ${clase}`;
        p.textContent = t;
        this.programar();
    }

    /** Pide un redibujo; varios cambios seguidos se dibujan una sola vez (WB5.6). */
    programar() {
        if (this.pendienteDibujo) return;
        this.pendienteDibujo = true;
        setTimeout(() => { this.pendienteDibujo = false; this.dibujar(); }, 50);
    }

    dibujar() {
        if (this.host) this.host.vencer();
        const nodo = this.host ? this.host.nodos.get(this.config.nodo) : null;
        const disp = nodo ? nodo.dispositivos.get(this.config.dispositivo) : null;
        this._tramos(nodo, disp);
        this._dispositivo(nodo, disp);
        this._nodo(nodo);
        if (this.host) {
            const c = this.host.contadores;
            $("contadores").textContent = `${c.mensajes} mensajes · ${c.saltosSeq} saltos de seq · ${c.rebirthsPedidos} Rebirth pedidos · ${c.malformados} mal formados`;
        }
    }

    // -----------------------------------------------------------------------------------------
    // WB5.2 Cadena por tramos y diagnóstico de la falla
    // -----------------------------------------------------------------------------------------

    _tramos(nodo, disp) {
        const t = {};
        const s = this.estadoSesion.estado;
        const protocolo = String(this.estadoSesion.detalle).startsWith("wss:") ? "WSS" : "WS";
        t.web = s === "conectado" ? ["ok", `${protocolo} conectada`] : s === "conectando" ? ["aviso", "conectando…"]
            : s === "desconectado" ? ["falla", "sin conexión"] : s === "rechazado" ? ["falla", "rechazada"]
                : ["desconocido", "sin conectar"];
        const metrica = (n) => (nodo && nodo.metricas.get(n)) || null;
        const enLinea = nodo && nodo.estado === ESTADO.EN_LINEA;
        if (t.web[0] !== "ok") {
            t.internet = ["desconocido", "—"];
        } else if (!nodo || nodo.estado === ESTADO.DESCONOCIDO) {
            t.internet = ["desconocido", "esperando NBIRTH"];
        } else {
            t.internet = enLinea ? ["ok", `en línea (bdSeq ${nodo.bdSeq})`] : ["falla", "NDEATH"];
        }
        const opc = metrica("Status/OPCUA/Estado");
        if (t.internet[0] !== "ok" || !opc) t.ethernet = ["desconocido", "—"];
        else t.ethernet = opc.valor === "DONE" ? ["ok", "DONE"] : ["falla", "OPC UA ERROR"];
        const mb = metrica("Status/MODBUS/Estado");
        const crc = metrica("Status/MODBUS/CRC_ERROR");
        if (t.ethernet[0] !== "ok" || !mb) t.rs485 = ["desconocido", "—"];
        else if (mb.valor === "DONE" && (!disp || disp.estado !== ESTADO.FUERA)) t.rs485 = ["ok", "DONE"];
        else if (mb.valor === "CRC ERROR") t.rs485 = ["aviso", `CRC ERROR (${crc ? crc.valor : "?"})`];
        else if (mb.valor === "DEVICE NOT REACHED" || (disp && disp.estado === ESTADO.FUERA)) t.rs485 = ["falla", mb.valor === "DONE" ? "DDEATH" : "DEVICE NOT REACHED"];
        else t.rs485 = ["aviso", String(mb.valor)];

        for (const [clave, [clase, txt]] of Object.entries(t)) {
            const el = $(`tramo-${clave}`);
            el.className = `tramo ${clase}`;
            el.querySelector(".estado-tramo").textContent = txt;
        }
        // Diagnóstico: el primer tramo con problema, de la web hacia el esclavo.
        const diag = $("diagnostico");
        let clase = "ok";
        let mensaje = "Toda la cadena funciona: esclavo → disp. 2 → disp. 3 → broker → web.";
        if (t.web[0] !== "ok") {
            clase = t.web[0] === "aviso" ? "aviso" : t.web[0] === "falla" ? "falla" : "desconocido";
            mensaje = s === "rechazado" ? `La web no pudo conectarse: ${this.estadoSesion.detalle}.`
                : s === "desconectado" ? "Falla en el tramo web ↔ broker: la página perdió la conexión (reintenta sola cada 2 s)."
                    : s === "conectando" ? "Conectando con el broker…" : "Conectate al broker para ver el estado.";
        } else if (t.internet[0] === "desconocido") {
            clase = "desconocido";
            mensaje = "Conectada. Esperando que el Edge Node publique su NBIRTH (o el Rebirth pedido).";
        } else if (t.internet[0] === "falla") {
            clase = "falla";
            mensaje = `Falla en el tramo disp. 3 ↔ broker: NDEATH a las ${hora(nodo.desde)}. El Edge Node se desconectó, o se perdió el disp. 3 o su conexión a internet (Last Will).`;
        } else if (t.ethernet[0] === "falla") {
            clase = "falla";
            mensaje = "Falla en el tramo disp. 2 ↔ disp. 3 (Ethernet, OPC UA): OPC UA ERROR. Cable desconectado o servidor OPC UA caído.";
        } else if (t.rs485[0] === "falla") {
            clase = "falla";
            mensaje = "Falla en el tramo disp. 1 ↔ disp. 2 (RS485, MODBus): DEVICE NOT REACHED. El esclavo no responde.";
        } else if (t.rs485[0] === "aviso") {
            clase = "aviso";
            mensaje = `Falla en el tramo disp. 1 ↔ disp. 2 (RS485, MODBus): ${t.rs485[1]}. Las respuestas llegan con el CRC errado o incompletas; a las 3 seguidas, los datos del dispositivo quedan en STALE.`;
        } else if (t.ethernet[0] === "desconocido" || t.rs485[0] === "desconocido") {
            clase = "desconocido";
            mensaje = "Edge Node en línea; esperando el estado de OPC UA y MODBus.";
        }
        diag.className = `diagnostico ${clase}`;
        diag.textContent = mensaje;
    }

    // -----------------------------------------------------------------------------------------
    // WB5.3 Dispositivo: entradas y salidas (armado con el DBIRTH)
    // -----------------------------------------------------------------------------------------

    _dispositivo(nodo, disp) {
        const pastilla = $("estado-dispositivo");
        const est = disp ? disp.estado : ESTADO.DESCONOCIDO;
        pastilla.className = `pastilla ${est === ESTADO.EN_LINEA ? "ok" : est === ESTADO.FUERA ? "falla" : "desconocido"}`;
        pastilla.textContent = disp ? (est === ESTADO.EN_LINEA ? "en línea" : est === ESTADO.FUERA ? `fuera de línea desde ${hora(disp.desde)}` : "sin datos")
            : "sin datos";
        const res = nodo && nodo.metricas.get("Status/Comandos/Resultado");
        $("resultado-comando").textContent = res && res.valor ? res.valor : "—";

        const contenedor = $("metricas-dispositivo");
        if (!disp || disp.metricas.size === 0) {
            if (this.firmaDispositivo !== "") {
                contenedor.innerHTML = '<p class="vacio">Esperando el DBIRTH del dispositivo.</p>';
                this.tarjetas.clear();
                this.firmaDispositivo = "";
            }
            return;
        }
        const nombres = [...disp.metricas.keys()].sort((a, b) => (a.startsWith("Outputs/") - b.startsWith("Outputs/")) || a.localeCompare(b));
        const firma = nombres.map(n => {
            const m = disp.metricas.get(n);
            return `${n}:${m.datatype}:${m.propiedades.Escribible}:${m.propiedades.ModbusRef}`;
        }).join("|");
        if (firma !== this.firmaDispositivo) {
            contenedor.innerHTML = "";
            this.tarjetas.clear();
            for (const n of nombres) {
                const t = this._tarjeta(n, disp.metricas.get(n));
                this.tarjetas.set(n, t);
                contenedor.appendChild(t.raiz);
            }
            this.firmaDispositivo = firma;
        }
        const habilitado = est === ESTADO.EN_LINEA && this.estadoSesion.estado === "conectado";
        for (const n of nombres) this._actualizarTarjeta(this.tarjetas.get(n), disp.metricas.get(n), habilitado);
    }

    _tarjeta(nombre, m) {
        const raiz = document.createElement("div");
        raiz.className = "metrica";
        raiz.dataset.metrica = nombre;
        const escribible = m.propiedades.Escribible === true;
        const min = m.propiedades.Minimo;
        const max = m.propiedades.Maximo;
        const corto = nombre.split("/").pop();
        raiz.innerHTML = `<div class="nombre"><span>${nombre.startsWith("Outputs/") ? "Salida" : "Entrada"} · <strong></strong></span><span class="chip"></span></div>
            <div class="valor"></div><div class="detalle"></div>`;
        raiz.querySelector("strong").textContent = corto;
        const t = { raiz, nombre, escribible, valor: raiz.querySelector(".valor"), chip: raiz.querySelector(".chip"), detalle: raiz.querySelector(".detalle") };
        if (m.datatype === TIPOS.Boolean) {
            t.lampara = document.createElement("span");
            t.lampara.className = "lampara";
            t.texto = document.createElement("span");
            t.valor.append(t.lampara, t.texto);
            if (escribible) {
                t.boton = document.createElement("button");
                t.boton.type = "button";
                t.boton.addEventListener("click", () => this.acciones.escribir(nombre, !t.ultimoValor));
                raiz.appendChild(t.boton);
            }
        } else {
            t.texto = document.createElement("span");
            t.valor.append(t.texto);
            const real = m.datatype === TIPOS.Float || m.datatype === TIPOS.Double;
            const numerico = real || [TIPOS.Int8, TIPOS.Int16, TIPOS.Int32, TIPOS.Int64, TIPOS.UInt8,
                TIPOS.UInt16, TIPOS.UInt32, TIPOS.UInt64].includes(m.datatype);
            const conRango = typeof min === "number" && typeof max === "number" && max > min;
            if (conRango) t.rango = [min, max];
            if (escribible && numerico && conRango) {
                // Salida analógica con rango: deslizador (continuo para Float y Double).
                t.deslizador = document.createElement("input");
                t.deslizador.type = "range";
                t.deslizador.min = String(min);
                t.deslizador.max = String(max);
                t.deslizador.step = real ? "any" : "1";
                t.deslizador.setAttribute("aria-label", corto);
                t.deslizador.addEventListener("input", () => {
                    this.ultimoUso.set(nombre, Date.now());
                    this.acciones.escribir(nombre, Number(t.deslizador.value));
                });
                raiz.appendChild(t.deslizador);
            } else if (escribible && numerico) {
                // Sin rango conocido: un campo y un botón (la web valida el tipo antes de mandar).
                const fila = document.createElement("div");
                fila.className = "fila-envio";
                t.campo = document.createElement("input");
                t.campo.type = "number";
                t.campo.step = real ? "any" : "1";
                t.campo.setAttribute("aria-label", corto);
                t.boton = document.createElement("button");
                t.boton.type = "button";
                t.boton.textContent = "Enviar";
                t.boton.addEventListener("click", () => {
                    if (t.campo.value !== "") this.acciones.escribir(nombre, Number(t.campo.value));
                });
                fila.append(t.campo, t.boton);
                raiz.appendChild(fila);
            } else if (conRango) {
                t.barra = document.createElement("div");
                t.barra.className = "barra";
                t.barra.appendChild(document.createElement("div"));
                raiz.insertBefore(t.barra, t.detalle);
            }
        }
        return t;
    }

    _actualizarTarjeta(t, m, habilitado) {
        t.ultimoValor = m.valor;
        const stale = m.calidad !== CALIDAD.GOOD;
        t.raiz.classList.toggle("stale", stale);
        t.chip.textContent = stale ? nombreCalidad(m.calidad) : "";
        t.chip.className = `chip ${m.calidad === CALIDAD.BAD ? "falla" : stale ? "desconocido" : ""}`;
        if (m.datatype === TIPOS.Boolean) {
            t.lampara.classList.toggle("encendida", m.valor === true);
            t.texto.textContent = m.valor === true ? "ON" : m.valor === false ? "OFF" : "—";
            if (t.boton) {
                t.boton.textContent = m.valor ? "Apagar" : "Encender";
                t.boton.disabled = !habilitado;
            }
        } else {
            t.texto.textContent = texto(m.valor, m.datatype);
            if (t.barra && t.rango && typeof m.valor === "number") {
                const pct = Math.min(100, Math.max(0, 100 * (m.valor - t.rango[0]) / (t.rango[1] - t.rango[0])));
                t.barra.firstChild.style.width = `${pct}%`;
            }
            if (t.campo) {
                t.campo.disabled = !habilitado;
                t.boton.disabled = !habilitado;
            }
            if (t.deslizador) {
                t.deslizador.disabled = !habilitado;
                // Mientras el usuario lo mueve (o hace menos de 1,5 s), no se lo pisa con el valor confirmado.
                const enUso = Date.now() - (this.ultimoUso.get(t.nombre) || 0) < 1500;
                if (!enUso && typeof m.valor === "number") t.deslizador.value = String(m.valor);
            }
        }
        const rango = t.rango ? ` · ${t.rango[0]} a ${t.rango[1]}` : "";
        const ref = typeof m.propiedades.ModbusRef === "string" ? ` · ${m.propiedades.ModbusRef}` : "";
        t.detalle.textContent = `${NOMBRE_DE_TIPO[m.datatype]}${rango}${ref} · hace ${hace(m.recibida)}`;
    }

    // -----------------------------------------------------------------------------------------
    // WB5.4 Edge Node: estado de cada capa
    // -----------------------------------------------------------------------------------------

    _nodo(nodo) {
        const pastilla = $("estado-nodo");
        const est = nodo ? nodo.estado : ESTADO.DESCONOCIDO;
        pastilla.className = `pastilla ${est === ESTADO.EN_LINEA ? "ok" : est === ESTADO.FUERA ? "falla" : "desconocido"}`;
        pastilla.textContent = est === ESTADO.EN_LINEA ? `en línea desde ${hora(nodo.desde)}`
            : est === ESTADO.FUERA ? `fuera de línea desde ${hora(nodo.desde)}` : "sin datos";
        $("boton-rebirth").disabled = this.estadoSesion.estado !== "conectado";
        for (const [id, filas] of Object.entries(CAPAS)) {
            const dl = $(id);
            if (dl.childElementCount === 0) {
                for (const [etiqueta, nombre, ancha] of filas) {
                    const dt = document.createElement("dt");
                    dt.textContent = etiqueta;
                    const dd = document.createElement("dd");
                    dd.dataset.metrica = nombre;
                    if (ancha) {
                        dt.classList.add("ancha");
                        dd.classList.add("ancha");
                    }
                    dl.append(dt, dd);
                }
                if (id === "capa-sparkplug") {
                    for (const [etiqueta, clave] of [["seq (último)", "seq"], ["Saltos de seq", "saltos"], ["Rebirth pedidos por la web", "pedidos"]]) {
                        const dt = document.createElement("dt");
                        dt.textContent = etiqueta;
                        const dd = document.createElement("dd");
                        dd.dataset.propio = clave;
                        dl.append(dt, dd);
                    }
                }
            }
            for (const dd of dl.querySelectorAll("dd[data-metrica]")) {
                const m = nodo && nodo.metricas.get(dd.dataset.metrica);
                dd.textContent = m ? texto(m.valor, m.datatype) : "—";
                dd.classList.toggle("stale", !!m && m.calidad !== CALIDAD.GOOD);
                const v = m ? m.valor : null;
                dd.className = dd.className.replace(/\b(ok|aviso|falla)\b/g, "").trim();
                if (dd.dataset.metrica === "Status/MODBUS/Estado" || dd.dataset.metrica === "Status/OPCUA/Estado") {
                    if (v === "DONE") dd.classList.add("ok");
                    else if (v === "CRC ERROR") dd.classList.add("aviso");
                    else if (v) dd.classList.add("falla");
                }
            }
            for (const dd of dl.querySelectorAll("dd[data-propio]")) {
                const k = dd.dataset.propio;
                dd.textContent = !nodo ? "—" : k === "seq" ? String(nodo.ultimoSeq ?? "—") : k === "saltos" ? String(nodo.saltosSeq) : String(nodo.rebirthsPedidos);
            }
        }
        // Tabla con todas las métricas del nodo
        const cuerpo = $("tabla-nodo").tBodies[0];
        if (!nodo) return;
        for (const [nombre, m] of nodo.metricas) {
            let fila = this.filasNodo.get(nombre);
            if (!fila) {
                fila = cuerpo.insertRow();
                for (let i = 0; i < 5; i++) fila.insertCell();
                fila.cells[0].textContent = nombre;
                fila.cells[4].className = "num";
                this.filasNodo.set(nombre, fila);
            }
            fila.cells[1].textContent = texto(m.valor, m.datatype);
            fila.cells[2].textContent = NOMBRE_DE_TIPO[m.datatype];
            fila.cells[3].textContent = nombreCalidad(m.calidad);
            fila.cells[4].textContent = hace(m.recibida);
        }
    }

    // -----------------------------------------------------------------------------------------
    // WB5.5 Registro de eventos
    // -----------------------------------------------------------------------------------------

    /**
     * Agrega un evento arriba de la lista.
     * @param {string} tipo   birth | death | seq | rebirth | cmd | ok | error | info | sesion
     * @param {string} txt
     */
    evento(tipo, txt) {
        const lista = $("eventos");
        const li = document.createElement("li");
        li.className = tipo;
        const t = document.createElement("time");
        t.textContent = hora(Date.now());
        const s = document.createElement("span");
        s.textContent = txt;
        li.append(t, s);
        lista.prepend(li);
        while (lista.childElementCount > MAX_EVENTOS) lista.lastElementChild.remove();
    }
}
