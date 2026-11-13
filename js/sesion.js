// =============================================================================================
// sesion.js — Sesión MQTT de la Host Application y su STATE (frente B, bloque WB2)
//
// La web es la Host Application de Sparkplug B (host_id «WebPdCI»). Se conecta al broker por MQTT
// sobre WebSocket con TLS (WSS), con MQTT 5, y publica su estado en spBv1.0/STATE/WebPdCI.
//
// Reglas de la especificación Sparkplug 3.0.0 que se cumplen acá:
//   - CONNECT con Clean Start = true y Session Expiry Interval = 0
//     (tck-id-message-flow-phid-sparkplug-clean-session-50).
//   - Will en el CONNECT: tópico spBv1.0/STATE/<host_id>, JSON {"online": false, "timestamp": T},
//     QoS 1, retain true, con T = la hora del CONNECT (tck-id-host-topic-phid-death-*,
//     tck-id-payloads-state-will-message-*).
//   - Apenas conecta, se suscribe al grupo y a su propio STATE, y recién después publica el birth
//     {"online": true, "timestamp": T}, QoS 1, retain true, con el mismo T del Will
//     (tck-id-message-flow-phid-sparkplug-subscription, -state-publish, -state-publish-payload).
//     El birth es el primer mensaje que publica en cada conexión (tck-id-host-topic-phid-birth-message):
//     los comandos esperan al birth, y al perder la conexión se descarta lo que MQTT.js tenía
//     pendiente de reenviar.
//   - Si después del birth le llega su propio STATE con online = false (no retenido), lo vuelve a
//     publicar online con el mismo T (tck-id-message-flow-hid-sparkplug-state-message-delivered).
//   - Cierre intencional: STATE offline con la hora actual y después DISCONNECT
//     (tck-id-host-topic-phid-death-payload-disconnect-clean).
//
// Librería: MQTT.js 5.16.1 (npm «mqtt», licencia MIT), archivo lib/mqtt.min.js; crea la variable
// global `mqtt`. Funciones usadas:
//   mqtt.connect(url: string, opciones: object) -> MqttClient   (se conecta solo, sin bloquear)
//     Opciones usadas: protocolVersion 5; clean true; properties.sessionExpiryInterval 0;
//     clientId (string); username, password (string; MQTT.js la conserva en options.password para
//     reconectar); keepalive (s); connectTimeout (ms); reconnectPeriod (ms); resubscribe false (las
//     suscripciones se hacen en cada «connect»); queueQoSZero false (un comando no se guarda para
//     mandarlo tarde); will {topic, payload, qos, retain}.
//   cliente.on(evento, función). Eventos usados:
//     "connect"   (connack)                CONNACK aceptado
//     "reconnect" ()                       se emite justo antes de cada nuevo intento: el CONNECT
//                                          se arma con cliente.options.will en ese momento
//                                          (verificado en build/lib/client.js, _reconnect y connect)
//     "close"     ()                       se cerró la conexión
//     "error"     (Error con .code)        CONNACK rechazado (≥ 0x80 en MQTT 5; 134 usuario o
//                                          contraseña, 135 no autorizado): MQTT.js deja de reintentar
//     "message"   (topico: string, carga: Uint8Array, paquete: {retain: boolean, qos: number})
//   cliente.subscribe(filtros: object {filtro: {qos}}, cb(err, concedidas))
//       err.code es el código del SUBACK si alguno vino rechazado (≥ 0x80, por ejemplo 0x87).
//   cliente.publish(topico: string, carga: string|Uint8Array, {qos, retain}, cb(err))
//       con QoS 1, cb recibe un error si el PUBACK trae un código de rechazo.
//   cliente.removeOutgoingMessage(id: number)   descarta un QoS 1 que esperaba su PUBACK
//   cliente.end(forzar: boolean, opciones: object, cb())   DISCONNECT y cierre; sin forzar, espera
//       los PUBACK pendientes; desde que se llama, MQTT.js no reintenta más.
//
// Bloques:
//   WB2.1 Conexión (Will STATE)            WB2.2 Al conectar: suscripciones y birth STATE
//   WB2.3 Reconexión: Will nuevo           WB2.4 STATE propio offline → se republica online
//   WB2.5 Recepción y comandos             WB2.6 Cierre intencional
// =============================================================================================

/* global mqtt */

const RECHAZOS_CONNACK = {
    4: "usuario o contraseña incorrectos", 5: "no autorizado",
    134: "usuario o contraseña incorrectos", 135: "no autorizado", 138: "cliente bloqueado",
    151: "cuota del broker excedida",
};

export class SesionMQTT {
    /**
     * @param {object} config
     * @param {string} config.url        p. ej. wss://<host>:8084/mqtt
     * @param {string} config.usuario
     * @param {string} config.grupo      group_id (UNRaf_PdCI)
     * @param {string} config.hostId     host_id (WebPdCI)
     * @param {number} [config.keepaliveS=30]
     * @param {string} clave             contraseña: va solo al CONNECT (MQTT.js la conserva en
     *                                   memoria para reconectar, y principal.js la borra al
     *                                   terminar la sesión, D49); no se guarda en el navegador
     * @param {object} avisos            funciones que llama la sesión
     * @param {(estado: string, detalle: string) => void} avisos.alEstado
     *        estado: "conectando" | "conectado" | "desconectado" | "rechazado" | "cerrado"
     * @param {(topico: string, carga: Uint8Array, retenido: boolean) => void} avisos.alMensaje
     * @param {(texto: string) => void} avisos.alRegistro
     */
    constructor(config, clave, avisos) {
        this.config = { keepaliveS: 30, ...config };
        this._clave = clave;
        this.avisos = avisos;
        this.topicoState = `spBv1.0/STATE/${config.hostId}`;
        this.cliente = null;
        this.marcaConnect = null;     // T: timestamp del Will del CONNECT vigente
        this.birthPublicado = false;  // el birth STATE de esta conexión ya salió (PUBACK recibido)
        this.conectado = false;
        this.cerrando = false;
        this.conexiones = 0;          // CONNACK aceptados desde que se abrió la página
        this.ultimoRepublicado = 0;
        this.republicarPendiente = null;
    }

    // -----------------------------------------------------------------------------------------
    // WB2.1 Conexión (Will STATE)
    // -----------------------------------------------------------------------------------------

    /** Arranca la conexión. MQTT.js reintenta solo cada 2 s si se corta (WB2.3). */
    conectar() {
        this.marcaConnect = Date.now();
        const id = `${this.config.hostId}-${Math.random().toString(16).slice(2, 10)}`;
        this.cliente = mqtt.connect(this.config.url, {
            protocolVersion: 5,
            clean: true,
            properties: { sessionExpiryInterval: 0 },
            clientId: id,
            username: this.config.usuario,
            password: this._clave,
            keepalive: this.config.keepaliveS,
            connectTimeout: 10000,
            reconnectPeriod: 2000,
            resubscribe: false,
            queueQoSZero: false,
            will: { topic: this.topicoState, payload: this._state(false, this.marcaConnect), qos: 1, retain: true },
        });
        this._clave = null;
        this.avisos.alEstado("conectando", this.config.url);
        this.avisos.alRegistro(`WB2.1 Conectando a ${this.config.url} como ${this.config.usuario} (MQTT 5, client id ${id})`);
        const cliente = this.cliente;
        cliente.on("connect", () => this._alConectar(cliente));
        cliente.on("reconnect", () => this._alReconectar());
        cliente.on("close", () => this._alCerrar());
        cliente.on("error", e => this._alError(e));
        cliente.on("message", (t, c, p) => this._alRecibir(t, c, p));
    }

    _state(online, marca) {
        return JSON.stringify({ online, timestamp: marca });
    }

    /** Corta el cliente sin reintentos y avisa el motivo. */
    _rechazar(motivo) {
        this.conectado = false;
        this.birthPublicado = false;
        this.avisos.alEstado("rechazado", motivo);
        this.cliente.end(true);
    }

    // -----------------------------------------------------------------------------------------
    // WB2.2 Al conectar: suscripciones y después el birth STATE
    // -----------------------------------------------------------------------------------------

    _alConectar(cliente) {
        if (this.cerrando) return;
        this.conectado = true;
        this.conexiones += 1;
        this.birthPublicado = false;
        this.avisos.alRegistro(`WB2.2 CONNACK aceptado (conexión ${this.conexiones}); suscribiendo`);
        const filtros = { [`spBv1.0/${this.config.grupo}/#`]: { qos: 1 }, [this.topicoState]: { qos: 1 } };
        const conexion = this.conexiones;
        cliente.subscribe(filtros, (err) => {
            // Si mientras tanto se cerró o se cortó, el birth ya no corresponde.
            if (!this.conectado || this.cerrando || conexion !== this.conexiones) return;
            if (err) {
                this.avisos.alRegistro(`WB2.2 Suscripción rechazada por el broker: ${err.message} (código ${err.code})`);
                this._rechazar(`suscripción rechazada (${err.message})`);
                return;
            }
            this._publicarBirth("birth");
        });
    }

    _publicarBirth(motivo) {
        const carga = this._state(true, this.marcaConnect);
        const conexion = this.conexiones;
        this.cliente.publish(this.topicoState, carga, { qos: 1, retain: true }, (err) => {
            if (!this.conectado || this.cerrando || conexion !== this.conexiones) return;   // conexión vieja
            if (err) {
                this.avisos.alRegistro(`WB2.2 STATE (${motivo}) rechazado por el broker: ${err.message}`);
                if (motivo === "birth") this._rechazar(`STATE rechazado (${err.message})`);
                return;
            }
            this.birthPublicado = true;
            this.avisos.alRegistro(`WB2.2 STATE ${carga} publicado (${motivo}, QoS 1, retenido)`);
            if (motivo === "birth") this.avisos.alEstado("conectado", this.config.url);
        });
    }

    // -----------------------------------------------------------------------------------------
    // WB2.3 Reconexión: cada CONNECT lleva un Will con la hora de ese CONNECT
    // -----------------------------------------------------------------------------------------

    _alReconectar() {
        this.marcaConnect = Date.now();
        this.cliente.options.will.payload = this._state(false, this.marcaConnect);
        // Tras una conexión que funcionó, sigue siendo «sin conexión» mientras reintenta.
        if (this.conexiones === 0) this.avisos.alEstado("conectando", "reintentando");
        else this.avisos.alEstado("desconectado", "reintentando cada 2 s");
        this.avisos.alRegistro("WB2.3 Reintentando la conexión (Will con la hora de este CONNECT)");
    }

    _alCerrar() {
        const estaba = this.conectado;
        this.conectado = false;              // antes de descartar: los callbacks ven la conexión cerrada
        this.birthPublicado = false;
        // Lo que MQTT.js no llegó a confirmar (por ejemplo un STATE) no se reenvía en la próxima
        // conexión: el primer mensaje de cada conexión tiene que ser su birth.
        for (const id of Object.keys(this.cliente.outgoing)) this.cliente.removeOutgoingMessage(Number(id));
        if (estaba) {
            this.avisos.alRegistro("WB2.3 Conexión con el broker perdida");
            this.avisos.alEstado("desconectado", "conexión perdida; reintentando cada 2 s");
        }
    }

    _alError(e) {
        const rechazo = typeof e.code === "number" && !this.conectado && (e.code >= 0x80 || (e.code >= 1 && e.code <= 5));
        if (rechazo) {
            const motivo = RECHAZOS_CONNACK[e.code] || e.message;
            this.avisos.alRegistro(`WB2.1 Conexión rechazada: ${motivo} (código ${e.code})`);
            this._rechazar(motivo);
        } else {
            this.avisos.alRegistro(`WB2.1 Error de MQTT: ${e.message}`);
        }
    }

    // -----------------------------------------------------------------------------------------
    // WB2.4 y WB2.5 Recepción y comandos
    // -----------------------------------------------------------------------------------------

    _alRecibir(topico, carga, paquete) {
        if (topico === this.topicoState) {
            this._alState(carga, paquete.retain);
            return;
        }
        this.avisos.alMensaje(topico, carga, paquete.retain);
    }

    _alState(carga, retenido) {
        let estado = null;
        try {
            estado = JSON.parse(new TextDecoder().decode(carga));
        } catch {
            estado = null;                       // carga vacía (retenido borrado) u otro contenido
        }
        if (!estado || estado.online !== false) return;
        // El retenido es la copia que había antes de suscribirse: el birth de esta conexión lo pisa.
        if (retenido || !this.birthPublicado) return;
        // WB2.4: alguien (el Will de otra pestaña, otro cliente) dejó «offline» en nuestro STATE.
        this.avisos.alRegistro(`WB2.4 Llegó STATE offline propio (timestamp ${estado.timestamp}): se republica online`);
        const espera = Math.max(0, this.ultimoRepublicado + 1000 - Date.now());   // a lo sumo uno por segundo
        if (this.republicarPendiente) return;
        this.republicarPendiente = setTimeout(() => {
            this.republicarPendiente = null;
            if (!this.conectado || this.cerrando) return;
            this.ultimoRepublicado = Date.now();
            this._publicarBirth("republicado");
        }, espera);
    }

    /**
     * Publica un comando (NCMD o DCMD): QoS 0, sin retain (tck-id-topics-ncmd-mqtt, -dcmd-mqtt).
     * @param {string} topico
     * @param {Uint8Array} carga
     * @returns {boolean} false si la sesión no está lista (sin conexión, o el birth todavía no salió):
     *                    el comando no se guarda para después
     */
    publicarComando(topico, carga) {
        if (!this.conectado || !this.birthPublicado || this.cerrando) return false;
        this.cliente.publish(topico, carga, { qos: 0, retain: false });
        return true;
    }

    // -----------------------------------------------------------------------------------------
    // WB2.6 Cierre intencional
    // -----------------------------------------------------------------------------------------

    /**
     * STATE offline con la hora actual (QoS 1, retenido) y después DISCONNECT: el broker descarta
     * el Will, por eso el offline va antes. MQTT.js espera el PUBACK del offline antes del
     * DISCONNECT; si en 3 s no llega (conexión muerta sin detectar), se cierra igual.
     * @returns {Promise<void>}
     */
    cerrar() {
        return new Promise((resolver) => {
            const cliente = this.cliente;
            if (!cliente) return resolver();
            const estabaConectado = this.conectado;
            this.cerrando = true;
            this.conectado = false;
            this.birthPublicado = false;
            if (this.republicarPendiente) clearTimeout(this.republicarPendiente);
            let listo = false;
            const terminar = (texto) => {
                if (listo) return;
                listo = true;
                this.avisos.alRegistro(texto);
                this.avisos.alEstado("cerrado", "desconectado a pedido");
                resolver();
            };
            if (!estabaConectado) {
                cliente.end(true, {}, () => terminar("WB2.6 Cliente cerrado (no estaba conectado)"));
                return;
            }
            const carga = this._state(false, Date.now());
            cliente.publish(this.topicoState, carga, { qos: 1, retain: true }, (err) => {
                this.avisos.alRegistro(err ? `WB2.6 STATE ${carga} sin confirmar (${err.message})`
                    : `WB2.6 STATE ${carga} publicado antes de desconectar`);
            });
            cliente.end(false, {}, () => terminar("WB2.6 DISCONNECT enviado"));
            setTimeout(() => {
                if (listo) return;
                if (cliente.stream) cliente.stream.destroy();   // sin PUBACK en 3 s: se corta el socket
                terminar("WB2.6 Sin respuesta del broker en 3 s: conexión cerrada sin DISCONNECT");
            }, 3000);
        });
    }
}
