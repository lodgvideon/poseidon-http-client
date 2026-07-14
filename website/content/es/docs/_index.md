---
title: Documentación
weight: 1
bookCollapseSection: false
---

# Documentación

poseidon-http-client es un cliente HTTP de bajo nivel para Go que implementa HTTP/1.1, HTTP/2 y HTTP/3 desde cero — con su propio framing, HPACK, QPACK y stack QUIC, sin `net/http` y sin librerías de protocolo de terceros. Las tres versiones del protocolo se usan a través de la misma API `Do`/`DoStream`: eliges el transporte y el código de la petición no cambia. Estas páginas cubren la instalación, una página por protocolo con un ejemplo verificado, el conjunto de funcionalidades común a todos los transportes (pooling, descubrimiento de servicios, reintentos, limitación de tasa, hooks, métricas) y un descargo de responsabilidad que conviene leer antes de usar una primera versión en cualquier entorno crítico para la seguridad.

- [Primeros pasos](getting-started/) — instalación, requisitos, primera petición.
- [HTTP/1.1](http1/) — transporte de conexión única, configuración del dialer TLS, fallback por ALPN.
- [HTTP/2](http2/) — clientes de conexión única, con pool y gestionados por descubrimiento; streaming y control de flujo.
- [HTTP/3](http3/) — clientes sobre QUIC, QPACK dinámico, control de congestión NewReno/BBR.
- [Funcionalidades](features/) — la API de peticiones, pooling, descubrimiento con Resolver/Selector, reintentos, limitación de tasa, hooks, métricas.
- [Descargo de responsabilidad](disclaimer/) — qué significa aquí "primera versión" y cómo reportar vulnerabilidades.
