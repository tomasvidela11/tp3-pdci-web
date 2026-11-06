// =============================================================================================
// principal.js — Arranque de la web (frente B, bloque WB0)
//
// Une las piezas:
//   carga.js    (WB1) protobuf de Sparkplug B
//   sesion.js   (WB2) MQTT sobre WSS y STATE de la Host Application
//   modelo.js   (WB3, WB4) nodos, dispositivos, métricas, seq, Rebirth y comandos
//   interfaz.js (WB5) pantalla
//
// Configuración: los valores de CONFIG. El Reorder Timeout se puede cambiar sin tocar el código
// con ?reorden=<ms> en la dirección de la página. El broker y el usuario se escriben en el
// formulario; los últimos usados quedan en el navegador (localStorage), la contraseña no. El broker
// no se toma de la dirección: un enlace preparado podría llevar la contraseña a otro servidor.
//
// Bloques: WB0.1 Configuración   WB0.2 Conexión y desconexión   WB0.3 Arranque
// =============================================================================================

import { cargarDefinicion } from "./carga.js";
import { HostSparkplug } from "./modelo.js";
import { SesionMQTT } from "./sesion.js";
import { Interfaz } from "./interfaz.js";

// ---------------------------------------------------------------------------------------------
// WB0.1 Configuración (identificadores de la decisión D5; broker de docs/BROKER.md)
// ---------------------------------------------------------------------------------------------

const CONFIG = {
    url: "wss://m112f218.ala.us-east-1.emqxsl.com:8084/mqtt",
    usuario: "web-host",
    grupo: "UNRaf_PdCI",
    hostId: "WebPdCI",
    nodo: "Dispositivo3",
    dispositivo: "Dispositivo1",
    reorderMs: 2000,        // Reorder Timeout (tck-id-operational-behavior-host-reordering-param)
    keepaliveS: 30,
};

function leerPreferencias() {
    try {
        const guardado = JSON.parse(localStorage.getItem("tp3-web") || "{}");
        if (guardado.url) CONFIG.url = guardado.url;
        if (guardado.usuario) CONFIG.usuario = guardado.usuario;
    } catch { /* sin almacenamiento: valores por defecto */ }
    const q = new URLSearchParams(location.search);
    if (q.get("reorden") && Number(q.get("reorden")) > 0) CONFIG.reorderMs = Number(q.get("reorden"));
}

function guardarPreferencias(url, usuario) {
    try { localStorage.setItem("tp3-web", JSON.stringify({ url, usuario })); } catch { /* nada */ }
}

/** La contraseña solo viaja cifrada: wss://, salvo un broker en la misma PC (pruebas). */
function motivoUrlInvalida(url) {
    let u;
    try { u = new URL(url); } catch { return "la dirección del broker no es válida"; }
    const local = u.hostname === "127.0.0.1" || u.hostname === "localhost";
    if (u.protocol === "wss:" || (u.protocol === "ws:" && local)) return null;
    return "el broker tiene que ser wss:// (con TLS): con ws:// la contraseña viajaría sin cifrar";
}

// ---------------------------------------------------------------------------------------------
// WB0.2 Conexión y desconexión
// ---------------------------------------------------------------------------------------------

let sesion = null;
let host = null;
let interfaz = null;

function conectar(url, usuario, clave) {
    if (sesion) return;
    const invalida = motivoUrlInvalida(url);
    if (invalida) {
        interfaz.evento("error", `No se conecta: ${invalida}`);
        interfaz.sesion("rechazado", invalida);
        return;
    }
    guardarPreferencias(url, usuario);
    // Cada conexión tiene su modelo y su sesión; los avisos de una sesión vieja no tocan la nueva.
    const h = new HostSparkplug(
        { grupo: CONFIG.grupo, reorderMs: CONFIG.reorderMs },
        (topico, carga) => s.publicarComando(topico, carga),
        (tipo, texto) => interfaz.evento(tipo, texto),
        () => interfaz.programar(),
    );
    const s = new SesionMQTT(
        { url, usuario, grupo: CONFIG.grupo, hostId: CONFIG.hostId, keepaliveS: CONFIG.keepaliveS }, clave,
        {
            alEstado: (estado, detalle) => {
                if (s !== sesion) return;
                if (estado === "desconectado") h.conexionPerdida();       // WB3.9
                // Al quedar en línea, Rebirth al Edge Node: así se tienen todos los valores enseguida,
                // aunque el nodo no publique nada nuevo (WB3.8).
                if (estado === "conectado") h.pedirRebirth(CONFIG.nodo, "la web se conectó y necesita el estado completo");
                interfaz.sesion(estado, detalle);
                if (estado === "rechazado") sesion = null;
            },
            alMensaje: (topico, carga) => { if (s === sesion) h.procesar(topico, carga); },
            alRegistro: (texto) => interfaz.evento("sesion", texto),
        },
    );
    sesion = s;
    host = h;
    interfaz.host = h;
    window.WB_APP.sesion = s;
    window.WB_APP.host = h;
    s.conectar();
}

async function desconectar() {
    if (!sesion) return;
    const s = sesion;
    await s.cerrar();
    if (sesion === s) sesion = null;
    if (host) host.conexionPerdida();
    interfaz.sesion("cerrado", "desconectado a pedido");
}

// ---------------------------------------------------------------------------------------------
// WB0.3 Arranque
// ---------------------------------------------------------------------------------------------

leerPreferencias();
interfaz = new Interfaz(CONFIG, {
    conectar,
    desconectar,
    escribir: (nombre, valor) => host && host.escribir(CONFIG.nodo, CONFIG.dispositivo, nombre, valor),
    rebirth: () => host && host.pedirRebirth(CONFIG.nodo, "pedido a mano", true),
});
// Acceso para depurar desde la consola del navegador y para las pruebas (web/pruebas/).
window.WB_APP = { config: CONFIG, interfaz, sesion: null, host: null, conectar, desconectar, listo: false };
interfaz.sesion("inicial", "");
try {
    await cargarDefinicion("sparkplug_b.proto");
    window.WB_APP.listo = true;
    interfaz.evento("sesion", "WB0.3 Definición sparkplug_b.proto cargada (protobufjs 8.8.1)");
} catch (e) {
    interfaz.evento("error", `No se pudo cargar sparkplug_b.proto: ${e.message}`);
    document.getElementById("boton-conectar").disabled = true;
}
