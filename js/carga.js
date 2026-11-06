// =============================================================================================
// carga.js — Carga Sparkplug B de la web (frente B, bloque WB1)
//
// Codifica y decodifica el payload protobuf de Sparkplug B 3.0.0 con la definición oficial de
// Eclipse Tahu (sparkplug_b.proto, la misma que usa el Edge Node en disp3/). Es el equivalente de
// disp3/carga_sparkplug.py del lado de la web.
//
// Librería: protobufjs 8.8.1 (npm «protobufjs», licencia BSD-3-Clause), archivo
// lib/protobuf.min.js (build completo: incluye el parser de .proto). Crea la variable global
// `protobuf`. Para los enteros de 64 bits usa long.js 5.3.2 (npm «long», Apache-2.0,
// lib/long.js, variable global `Long`), que se carga antes: sin él, protobufjs lee los uint64 como
// number y pierde los valores grandes y los Int64 negativos (lo encontró la revisión independiente).
// Funciones usadas:
//   protobuf.parse(fuente: string, opciones: {keepCase: boolean}) -> {root: Root}
//       keepCase: true conserva los nombres del .proto (int_value, boolean_value…); sin él, los
//       pasa a camelCase.
//   Root.lookupType(nombre: string) -> Type          Type del mensaje (lanza Error si no existe)
//   Type.decode(datos: Uint8Array) -> Message        lanza Error si el protobuf está mal formado
//   Type.toObject(m: Message, opciones) -> object    objeto JS plano; opciones usadas:
//       longs: String     los uint64 salen como texto decimal exacto (sin perder precisión)
//       enums: Number     los enum como número
//       oneofs: true      agrega el nombre del campo del oneof que vino («value»: "int_value")
//       defaults: false   no agrega los campos ausentes (para saber qué vino en el cable)
//   Type.create(propiedades: object) -> Message
//   Type.encode(m: Message).finish() -> Uint8Array
//
// Bloques:
//   WB1.1 Carga de la definición       WB1.2 Decodificación del payload
//   WB1.3 Valor según el datatype      WB1.4 Propiedades y Quality
//   WB1.5 Codificación de comandos (NCMD y DCMD)
// =============================================================================================

/* global protobuf */

// DataType de sparkplug_b.proto (enum DataType). Solo se usan los escalares.
export const TIPOS = {
    Int8: 1, Int16: 2, Int32: 3, Int64: 4, UInt8: 5, UInt16: 6, UInt32: 7, UInt64: 8,
    Float: 9, Double: 10, Boolean: 11, String: 12, DateTime: 13, Text: 14, UUID: 15,
    DataSet: 16, Bytes: 17, File: 18, Template: 19,
};
export const NOMBRE_DE_TIPO = Object.fromEntries(Object.entries(TIPOS).map(([k, v]) => [v, k]));

// Campo del oneof «value» en el que viaja cada datatype (especificación, cap. 6, «datatype»).
const CAMPO_DE_TIPO = {
    1: "int_value", 2: "int_value", 3: "int_value", 5: "int_value", 6: "int_value", 7: "int_value",
    4: "long_value", 8: "long_value", 13: "long_value",
    9: "float_value", 10: "double_value", 11: "boolean_value",
    12: "string_value", 14: "string_value", 15: "string_value",
    17: "bytes_value", 18: "bytes_value",
};
// Enteros con signo: viajan en complemento a dos con el ancho del tipo (D20, igual que Tahu).
const ANCHO_CON_SIGNO = { 1: 8, 2: 16, 3: 32, 4: 64 };
const RANGO_ENTERO = {
    1: [-128, 127], 2: [-32768, 32767], 3: [-2147483648, 2147483647],
    5: [0, 255], 6: [0, 65535], 7: [0, 4294967295],
    4: [Number.MIN_SAFE_INTEGER, Number.MAX_SAFE_INTEGER], 8: [0, Number.MAX_SAFE_INTEGER],
};

// Quality (cap. 6, «Quality Codes»): propiedad Int32 de la métrica. Sin ella, GOOD.
export const CALIDAD = { BAD: 0, GOOD: 192, STALE: 500 };

let Payload = null;

// ---------------------------------------------------------------------------------------------
// WB1.1 Carga de la definición (una vez, al abrir la página)
// ---------------------------------------------------------------------------------------------

/**
 * Lee sparkplug_b.proto del mismo sitio y prepara el tipo Payload.
 * @param {string} url  ruta del .proto (por defecto, junto a index.html)
 * @returns {Promise<void>}  lanza Error si no se pudo leer o interpretar
 */
export async function cargarDefinicion(url = "sparkplug_b.proto") {
    const respuesta = await fetch(url);
    if (!respuesta.ok) throw new Error(`no se pudo leer ${url} (HTTP ${respuesta.status})`);
    if (!protobuf.util.Long) throw new Error("falta long.js (tiene que cargarse antes que protobuf.min.js)");
    const raiz = protobuf.parse(await respuesta.text(), { keepCase: true }).root;
    Payload = raiz.lookupType("org.eclipse.tahu.protobuf.Payload");
}

// ---------------------------------------------------------------------------------------------
// WB1.2 Decodificación del payload
// ---------------------------------------------------------------------------------------------

/**
 * Decodifica un payload Sparkplug B. Los valores quedan «crudos» (campo y contenido): el datatype
 * puede no venir en los DATA, y se interpreta después con el del BIRTH (WB1.3).
 * @param {Uint8Array} datos
 * @returns {{timestamp: number|null, seq: number|null, metricas: Array<object>}}
 *   cada métrica: {nombre, alias, timestamp, datatype (o null), nulo, campo, crudo, propiedades}
 * @throws {Error} si el protobuf está mal formado
 */
export function decodificar(datos) {
    const m = Payload.toObject(Payload.decode(datos),
        { longs: String, enums: Number, oneofs: true, defaults: false });
    return {
        timestamp: m.timestamp !== undefined ? Number(m.timestamp) : null,
        seq: m.seq !== undefined ? Number(m.seq) : null,
        metricas: (m.metrics || []).map(x => ({
            nombre: x.name !== undefined ? x.name : null,
            alias: x.alias !== undefined ? Number(x.alias) : null,
            timestamp: x.timestamp !== undefined ? Number(x.timestamp) : null,
            datatype: x.datatype !== undefined ? x.datatype : null,
            nulo: x.is_null === true,
            campo: x.value || null,                      // nombre del campo del oneof que vino
            crudo: x.value ? x[x.value] : undefined,
            propiedades: x.properties ? leerPropiedades(x.properties) : {},
        })),
    };
}

// ---------------------------------------------------------------------------------------------
// WB1.3 Valor según el datatype
// ---------------------------------------------------------------------------------------------

/**
 * Convierte el valor crudo al tipo de JavaScript que corresponde al datatype.
 * @param {string|null} campo  campo del oneof que vino (int_value, long_value…)
 * @param {*} crudo            contenido del campo (los uint64 llegan como texto)
 * @param {number} datatype    datatype del BIRTH (o del propio mensaje)
 * @returns {boolean|number|string|null}
 * @throws {Error} si el valor viene en otro campo que el del datatype, o el tipo no se soporta
 */
export function valorSegunTipo(campo, crudo, datatype) {
    const esperado = CAMPO_DE_TIPO[datatype];
    if (!esperado) throw new Error(`datatype ${datatype} no soportado`);
    if (campo !== esperado) {
        throw new Error(`${NOMBRE_DE_TIPO[datatype]} debería venir en ${esperado} y vino en ${campo}`);
    }
    if (esperado === "long_value") {
        const n = BigInt(crudo);
        const v = ANCHO_CON_SIGNO[datatype] ? BigInt.asIntN(64, n) : BigInt.asUintN(64, n);
        return Number(v);                               // métricas de este sistema: |v| < 2^53
    }
    if (esperado === "int_value") {
        const bits = ANCHO_CON_SIGNO[datatype];
        if (!bits) return crudo >>> 0;
        const mascara = bits === 32 ? 0xFFFFFFFF : (1 << bits) - 1;
        const v = (crudo & mascara) >>> 0;
        return v >= 2 ** (bits - 1) ? v - 2 ** bits : v;
    }
    if (esperado === "float_value") return Number(Math.fround(crudo).toPrecision(7));
    return crudo;
}

/**
 * Valor de una métrica decodificada (WB1.2) con el datatype dado; null si viene con is_null.
 * @param {object} metrica  métrica de decodificar()
 * @param {number} datatype
 * @returns {boolean|number|string|null}
 */
export function valorDe(metrica, datatype) {
    if (metrica.nulo) return null;
    return valorSegunTipo(metrica.campo, metrica.crudo, datatype);
}

// ---------------------------------------------------------------------------------------------
// WB1.4 Propiedades y Quality
// ---------------------------------------------------------------------------------------------

/**
 * PropertySet → objeto nombre → valor. Las propiedades que no se pueden leer quedan en null.
 * @param {{keys: string[], values: object[]}} conjunto
 * @returns {Object<string, *>}
 */
function leerPropiedades(conjunto) {
    const salida = {};
    (conjunto.keys || []).forEach((clave, i) => {
        const p = (conjunto.values || [])[i] || {};
        try {
            salida[clave] = p.is_null ? null : valorSegunTipo(p.value || null, p.value ? p[p.value] : undefined, p.type);
        } catch {
            salida[clave] = null;
        }
    });
    return salida;
}

/**
 * Quality de una métrica: la propiedad «Quality» (Int32); sin ella, GOOD (cap. 6).
 * @param {object} metrica  métrica de decodificar()
 * @returns {number} 0 (BAD), 192 (GOOD) o 500 (STALE)
 */
export function calidadDe(metrica) {
    const q = metrica.propiedades.Quality;
    return typeof q === "number" ? q : CALIDAD.GOOD;
}

// ---------------------------------------------------------------------------------------------
// WB1.5 Codificación de comandos (NCMD y DCMD)
// ---------------------------------------------------------------------------------------------

/**
 * Comprueba que un valor entre en el datatype. Devuelve el motivo del rechazo, o null si entra.
 * @param {*} valor
 * @param {number} datatype
 * @returns {string|null}
 */
export function motivoFueraDeTipo(valor, datatype) {
    const nombre = NOMBRE_DE_TIPO[datatype] || `datatype ${datatype}`;
    if (datatype === TIPOS.Boolean) return typeof valor === "boolean" ? null : `${nombre} espera true o false`;
    if (datatype === TIPOS.String || datatype === TIPOS.Text) return typeof valor === "string" ? null : `${nombre} espera texto`;
    if (datatype === TIPOS.Float || datatype === TIPOS.Double) {
        return typeof valor === "number" && Number.isFinite(valor) ? null : `${nombre} espera un número`;
    }
    const rango = RANGO_ENTERO[datatype];
    if (!rango) return `${nombre} no se puede mandar desde la web`;
    if (!Number.isInteger(valor)) return `${nombre} espera un entero`;
    if (valor < rango[0] || valor > rango[1]) return `${nombre} va de ${rango[0]} a ${rango[1]}`;
    return null;
}

/**
 * Arma el payload de un NCMD o DCMD: timestamp del payload, sin seq (tck-id-payloads-ncmd-seq,
 * tck-id-payloads-dcmd-seq) y métricas sin datatype (tck-id-payloads-metric-datatype-not-req:
 * SHOULD NOT en los CMD); el valor va en el campo del datatype del BIRTH.
 * @param {Array<{nombre: string, datatype: number, valor: *}>} metricas
 * @param {number} marcaMs  hora de envío en ms desde 1970 (UTC)
 * @returns {Uint8Array}
 * @throws {Error} si un valor no entra en su datatype
 */
export function codificarComando(metricas, marcaMs) {
    const lista = metricas.map(({ nombre, datatype, valor }) => {
        const motivo = motivoFueraDeTipo(valor, datatype);
        if (motivo) throw new Error(`${nombre}: ${motivo}`);
        const campo = CAMPO_DE_TIPO[datatype];
        let v = valor;
        if (campo === "int_value") v = valor >>> 0;     // complemento a dos en 32 bits
        if (ANCHO_CON_SIGNO[datatype] && ANCHO_CON_SIGNO[datatype] < 32) v = valor & ((1 << ANCHO_CON_SIGNO[datatype]) - 1);
        return { name: nombre, timestamp: marcaMs, [campo]: v };
    });
    return Payload.encode(Payload.create({ timestamp: marcaMs, metrics: lista })).finish();
}
