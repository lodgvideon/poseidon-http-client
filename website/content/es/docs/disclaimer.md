---
title: Aviso legal
weight: 6
---

# Aviso legal

poseidon-http-client es software joven. Esta es su primera versión. Lea esta página antes de depender de él.

## Sin garantía

La biblioteca se distribuye bajo licencia MIT y se proporciona **tal cual**, sin garantía de ningún tipo. La usa bajo su propio riesgo. Consulte el archivo [LICENSE](https://github.com/lodgvideon/poseidon-http-client/blob/main/LICENSE) para los términos exactos.

## Código sensible a la seguridad, sin auditoría externa

Esta biblioteca implementa desde cero protocolos sensibles a la seguridad: el transporte QUIC (RFC 9000/9001/9002), la protección de registros TLS 1.3 para QUIC, HPACK (RFC 7541) y QPACK (RFC 9204). No reutiliza `net/http`, `quic-go` ni ninguna otra implementación de protocolo consolidada en estas rutas.

Lo que se ha hecho:

- ~200 tests de conformidad vinculados a secciones de los RFC, verificados en CI.
- Fuzzing de los parsers del formato de cable.
- Verificación de interoperabilidad contra tres implementaciones independientes de servidor HTTP/3 (Caddy/quic-go, nginx, aioquic) sobre UDP real.
- Toda la suite de tests se ejecuta bajo el detector de carreras de Go.

Lo que **no** se ha hecho: una auditoría de seguridad formal por terceros. Los tests y el fuzzing reducen la tasa de defectos; no demuestran la ausencia de vulnerabilidades en una pila de protocolos escrita desde cero.

## Antes de usarla en producción

No trate esta biblioteca como un reemplazo directo de `net/http` en sistemas críticos para la seguridad. Si su despliegue maneja pares no confiables o datos sensibles, o es crítico para la seguridad por cualquier otro motivo, revise usted mismo las rutas de código de las que dependa — o espere a que el proyecto acumule más historial en producción — antes de adoptarla.

Para generación de carga, testing y herramientas contra infraestructura que usted controla, el perfil de riesgo es menor. Ese es el caso de uso principal para el que está construida esta biblioteca.

## Reporte de vulnerabilidades

Si encuentra un problema de seguridad, repórtelo de forma privada como se describe en [SECURITY.md](https://github.com/lodgvideon/poseidon-http-client/blob/main/SECURITY.md). No abra un issue público de GitHub para vulnerabilidades.
