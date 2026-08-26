# nt-demo

Простой инструмент нагрузочного тестирования HTTP-сервисов на Go.

Генерирует запросы по одному из двух сценариев нагрузки, измеряет пропускную
способность и задержки ответов.

## Сценарии нагрузки

| Сценарий  | Флаг          | Описание                                                          |
| --------- | ------------- | ----------------------------------------------------------------- |
| `linear`  | `-scenario linear`  | Нагрузка плавно растёт от `-min-rps` до `-max-rps` за всё время теста |
| `sine`    | `-scenario sine`    | Нагрузка осциллирует по синусоиде: статичный минимум → пики → спад → снова статика, повторяется циклически |

Форма сигнала для `sine`:

- минимум (`-min-rps`) — базовый статичный уровень нагрузки;
- пики (`-peak-rps`) — максимальная нагрузка в верхней точке волны;
- период (`-period`) — длительность полного цикла волны.

## Использование

### Запуск локально

```bash
go build -o nt-demo .
./nt-demo -url http://api.example.com -scenario linear -duration 60s -min-rps 10 -max-rps 200
```

### Параметры

| Флаг               | По умолчанию              | Описание                                            |
| ------------------ | ------------------------- | --------------------------------------------------- |
| `-url`             | `http://localhost:8080`   | Целевой URL                                         |
| `-scenario`        | `linear`                  | Сценарий нагрузки: `linear` или `sine`              |
| `-duration`        | — (обязательный)          | Длительность теста, напр. `60s`, `2m`               |
| `-min-rps`         | `10`                      | Минимальная нагрузка, RPS                           |
| `-max-rps`         | `100`                     | Максимальная нагрузка, RPS (для `linear`)           |
| `-peak-rps`        | `200`                     | Пиковая нагрузка, RPS (для `sine`)                  |
| `-period`          | `duration / 4`            | Период sin-волны, напр. `20s`                       |
| `-concurrency`     | `50`                      | Максимум одновременных HTTP-запросов                |
| `-timeout`         | без таймаута              | Таймаут на запрос, напр. `5s`                       |

### Примеры

Линейный рост нагрузки за 2 минуты:

```bash
./nt-demo -url http://api.example.com -scenario linear -duration 2m \
  -min-rps 10 -max-rps 500 -concurrency 200
```

Волнообразная нагрузка с пиками каждые 30 секунд:

```bash
./nt-demo -url http://api.example.com -scenario sine -duration 5m \
  -min-rps 20 -peak-rps 1000 -period 30s -concurrency 300 -timeout 5s
```

## Вывод

Прогресс каждые 5 секунд:

```
  elapsed 10s  rps(avg) 29.0  failures 0
```

Итоговый отчёт:

```
=== Results ===
Duration:   12.001s
Requests:   319
Failures:   0
RPS avg:    26.58
Latency     p50: 1.743666ms  p90: 2.68675ms  p95: 2.905792ms  p99: 3.186208ms
```

- `Requests` / `RPS avg` — пропускная способность;
- `Failures` — число запросов, завершившихся ошибкой (таймаут, сеть, HTTP-код 4xx/5xx — учитывается как ошибка);
- `Latency p50/p90/p95/p99` — перцентили задержки ответа.

## Web UI

Помимо CLI инструмент умеет запускать веб-интерфейс, где можно настроить и
запустить нагрузочный тест, наблюдать за ним в реальном времени (RPS, ошибки,
график) и видеть итоговые перцентили латентности.

```bash
./nt-demo -server :8080
```

Открой в браузере `http://localhost:8080`.

В Docker:

```bash
docker run --rm -p 8080:8080 nt-demo -server :8080
```

## Docker

Сборка мультиплатформенного образа (amd64 + arm64) и публикация в
`ghcr.io/rLukoyanov/nt-demo` выполняются автоматически GitHub Actions
при пуше в `main` (тег `latest`) и при создании тегов `v*`.

Локальная сборка:

```bash
docker build -t nt-demo .
docker run --rm nt-demo -url http://api.example.com -scenario sine \
  -duration 2m -min-rps 20 -peak-rps 500 -period 30s
docker run --rm -p 8080:8080 nt-demo -server :8080   # веб-интерфейс
```

Запуск образа с GHCR на Linux-машине:

```bash
docker pull ghcr.io/rLukoyanov/nt-demo:latest
docker run --rm ghcr.io/rLukoyanov/nt-demo:latest \
  -url http://api.example.com -scenario linear -duration 60s \
  -min-rps 10 -max-rps 200
```

> Примечание: сетевая маршрутизация до целевого сервиса выполняется со стороны
> контейнера. Для тестирования локального сервиса запускайте контейнер
> с `--network host`.