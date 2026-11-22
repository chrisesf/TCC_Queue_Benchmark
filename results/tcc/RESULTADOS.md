# Resultados da campanha experimental (Cap. 5)

**Ambiente:** VM `vagoneta`, execução nativa (sem Docker)  
**Protocolo:** Seções 4.2, 4.3 e 4.6 do TCC  
**Topologia:** MPMC 4×4 (sem varrer número de núcleos)  
**Repetições:** N = 30 (média ± IC 95%, t de Student)  
**Testes de significância:** Welch ou Mann–Whitney a 95%  
**Chegadas:** rajada (`burst`, batch=1000, pause=1 ms) e equilibrado (`paced`)  
**Payloads:** 64 B, 1 KB, 64 KB  
**Filas em memória:** ring buffer com payload *inline*; mutex e Michael–Scott com *heap*  
**Fonte bruta:** `comparacao.txt`, `memoria.csv`, `brokers.csv`

Erros (perda + duplicação + fora de ordem + corrupção) ≈ 0 em quase todos os cenários. Exceção: Kafka *at-least-once* em 64 KB rajada, taxa ≈ 6,7×10⁻⁶.

---

## 1. Rajada — vazão (msg/s)

| Mecanismo | 64 B | 1 KB | 64 KB |
|---|---:|---:|---:|
| Ring buffer (inline) | **3 491 688 ± 50 613** | **2 752 656 ± 36 036** | 188 033 ± 1 620 |
| Michael–Scott (heap) | 1 919 688 ± 20 516 | 1 607 548 ± 10 452 | **455 057 ± 8 501** |
| Mutex (heap) | 1 254 873 ± 16 064 | 1 225 348 ± 11 429 | 447 844 ± 3 997 |
| Kafka at-least-once | 1 884 323 ± 39 789 | 1 024 599 ± 21 640 | 36 799 ± 601 |
| Kafka at-most-once | 1 758 635 ± 65 690 | 1 031 929 ± 23 508 | 35 569 ± 617 |
| RabbitMQ at-most-once | 285 598 ± 6 024 | 248 280 ± 5 359 | 66 503 ± 1 267 |
| RabbitMQ at-least-once | 95 178 ± 979 | 67 376 ± 674 | 36 539 ± 625 |
| Kafka exactly-once | 38 926 ± 14 | 38 737 ± 22 | 17 356 ± 140 |

### Latência na rajada (p50 / p99)

| Mecanismo | 64 B | 1 KB | 64 KB |
|---|---|---|---|
| Ring buffer | 30,4 µs / 142 µs | 52,8 µs / 122 µs | 21,6 µs / 30,3 µs |
| Michael–Scott | 334 µs / 695 µs | 187 µs / 499 µs | 1,8 µs / 18 µs |
| Mutex | 29,8 µs / 366 µs | 59,5 µs / 342 µs | 2,1 µs / 8,1 µs |
| Kafka at-least-once | 5,2 ms / 9,3 ms | 9,9 ms / 15,9 ms | 54 ms / 100 ms |
| Kafka at-most-once | 5,8 ms / 9,9 ms | 9,7 ms / 16,1 ms | 58 ms / 109 ms |
| RabbitMQ at-most-once | 52 ms / 98 ms | 31 ms / 57 ms | 4,4 ms / 17 ms |
| RabbitMQ at-least-once | 20 ms / 39 ms | 29 ms / 55 ms | 43 ms / 69 ms |
| Kafka exactly-once | 103 ms / 104 ms | 103 ms / 105 ms | 137 ms / 166 ms |

> Na rajada a latência inclui tempo parado na fila (Lei de Little). Interpretação principal: **vazão**.

---

## 2. Equilibrado — latência p50 (vazão ≈ taxa pedida)

Taxas totais pedidas: **8 000** msg/s (64 B), **2 000** msg/s (1 KB), **400** msg/s (64 KB).

| Mecanismo | 64 B p50 | 1 KB p50 | 64 KB p50 |
|---|---:|---:|---:|
| Ring buffer | **606 ns** | **801 ns** | 14,5 µs |
| Michael–Scott | 832 ns | 984 ns | **5,4 µs** |
| Mutex | 4,3 µs | 45 µs | 55 µs |
| RabbitMQ at-most-once | **90 µs** | **150 µs** | **535 µs** |
| RabbitMQ at-least-once | 152 µs | 177 µs | 638 µs |
| Kafka at-most-once | 3,24 ms | 5,20 ms | 5,65 ms |
| Kafka at-least-once | 3,21 ms | 5,20 ms | 5,67 ms |
| Kafka exactly-once | 352 ms | 602 ms | 1,10 s |

Vazão observada (todos ≈ taxa, exceto exactly-once um pouco abaixo):

| Mecanismo | 64 B | 1 KB | 64 KB |
|---|---:|---:|---:|
| Filas em memória | 8 004 | 2 004 | 402 |
| RabbitMQ | 7 996–8 003 | 2 003–2 004 | 402 |
| Kafka at-most / at-least | 7 974–7 975 | 1 992–1 993 | 401 |
| Kafka exactly-once | 7 256 | 1 816 | 380 |

---

## 3. Achados principais (diferenças significativas a 95%)

1. **Payload pequeno/médio, rajada:** ring buffer > Michael–Scott > mutex > Kafka > RabbitMQ > Kafka exactly-once.
2. **Payload 64 KB, rajada:** Michael–Scott ≈ mutex ≫ ring buffer (custo de cache do buffer pré-alocado grande).
3. **Alta vazão:** Kafka supera RabbitMQ por cerca de uma ordem de grandeza.
4. **Baixa carga (equilibrado):** RabbitMQ tem latência bem menor que Kafka (µs vs ms); filas em memória ficam em ns–µs.
5. **Exactly-once:** vazão limitada pelo commit transacional (~100 ms / lote de 1000) → ~39 k msg/s estáveis em 64 B/1 KB; latência p50 sobe para centenas de ms ou segundos no equilibrado.
6. **Corretude:** taxa de erro nula na prática; um único cenário com erro residual negligível (Kafka at-least, 64 KB rajada).

---

## 4. Ranking resumido por objetivo

| Objetivo | Melhor escolha (neste experimento) |
|---|---|
| Máxima vazão, mesma máquina, payload ≤ 1 KB | Ring buffer |
| Máxima vazão, payload 64 KB | Michael–Scott (ou mutex) |
| Menor latência sob carga moderada | Ring / Michael–Scott; entre brokers, RabbitMQ |
| Vazão com durabilidade/distribuição | Kafka (at-most ou at-least) |
| Semântica exactly-once | Kafka transacional (aceitar perda grande de vazão) |

---

## 5. Limitações

- Topologia fixa MPMC 4×4 (sem escala 1…32 threads).
- Brokers e clientes na mesma VM (sem rede real entre máquinas).
- CPU/memória do processo do broker não amostrados (execução sem Docker).
- Latência em modo rajada não isola o custo de enqueue/dequeue puro.
