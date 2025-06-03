# 📈 Custom Bloomberg DataSource Emulator for Apache Spark

This project implements a mock **Spark DataSource V1 connector** for Bloomberg BLPAPI-like data, including support for both **batch** and **streaming** reads. It is designed to simulate Bloomberg services such as:

- `HistoricalDataRequest`
- `ReferenceDataRequest`
- `IntradayTickRequest`
- `IntradayBarRequest`
- `MarketData (Streaming)` via `.readStream`

This emulator is useful for **offline testing**, **demoing**, and **developing against Bloomberg-like APIs** without requiring an actual Bloomberg B-PIPE connection.

---

## 📦 Project Structure

```
src/
├── main/
│   └── scala/
│       └── com/
│           └── example/
│               └── blp/
│                   ├── BlpRelation.scala
│                   ├── BlpStreamingSource.scala
│                   ├── DefaultSource.scala
```

---

## ✅ Features

- Supports `.read.format("//blp/refdata")` for batch data emulation.
- Supports `.readStream.format("//blp/mktdata")` for real-time streaming emulation.
- Handles typical Bloomberg request types with mock data:
  - Historical Data
  - Reference Data
  - Intraday Ticks
  - Intraday Bars
- Parameterized via `.option(...)` calls similar to the actual Bloomberg connector.

---

## 🛠️ Build Instructions

1. **Clone the repository** (or copy the source into your Spark plugin project).
2. Build using `sbt`:

```bash
sbt package
```

This generates a JAR under `target/scala-2.12/blp-datasource_2.12-<version>.jar`.

3. Upload the JAR to your Spark environment (e.g., Databricks, local cluster).

---

## 🧪 Usage Examples

### 🔹 Historical Data

```python
historical_df = (
    spark.read.format("//blp/refdata")
    .option("serviceName", "HistoricalDataRequest")
    .option("fields", "['BID','ASK']")
    .option("securities", "['SPY US EQUITY','MSFT US EQUITY']")
    .option("startDate", "2022-01-01")
    .load()
)
```

### 🔹 Reference Data

```python
reference_df = (
    spark.read.format("//blp/refdata")
    .option("serviceName", "ReferenceDataRequest")
    .option("fields", "['PX_LAST','BID','ASK']")
    .option("securities", "['SPY US EQUITY']")
    .option("overrides", "{'CHAIN_EXP_DT_OVRD':'20241220'}")
    .load()
)
```

### 🔹 Intraday Tick / Bar Data

```python
tick_df = (
    spark.read.format("//blp/refdata")
    .option("serviceName", "IntradayTickRequest")
    .option("security", "SPY US EQUITY")
    .option("fields", "['BID','ASK']")
    .option("startDateTime", "2022-11-01 09:30:00")
    .load()
)
```

### 🔹 Streaming Market Data

```python
stream_df = (
    spark.readStream.format("//blp/mktdata")
    .option("fields", "['BID','ASK','TRADE_UPDATE_STAMP_RT']")
    .option("securities", "['SPY US EQUITY','MSFT US EQUITY']")
    .load()
)
```

---

## ⚙️ Options Supported

| Option           | Description                              | Applies To                         |
|------------------|------------------------------------------|------------------------------------|
| `serviceName`     | Type of Bloomberg request                | Batch (`refdata`)                 |
| `security` / `securities` | Ticker(s) to query                  | All                                |
| `fields`         | List of fields to retrieve               | All                                |
| `startDate`, `startDateTime` | Date or timestamp filters        | Historical / Tick / Bar           |
| `interval`       | Bar interval in minutes                  | IntradayBarRequest                 |
| `overrides`      | JSON-style override map                  | ReferenceDataRequest               |
| `partitions`     | Partition hint (mocked)                  | All                                |

---

## 🧬 Schema

Each `serviceName` returns data with different schema:

- **HistoricalDataRequest**: `security`, `field`, `date`, `value`
- **IntradayTickRequest / BarRequest**: `security`, `field`, `timestamp`, `value`
- **ReferenceDataRequest**: `security`, `field`, `value`
- **Streaming MarketData**: same as Tick/Bar with real-time generation

---

## 📋 Notes

- This connector **does not access real Bloomberg data**.
- Use for **testing**, **demo**, or **teaching** purposes.
- Streaming data is randomly generated with `Random` seed.

---

## 👨‍💻 Maintainer

- Developed by [Your Name or Org]
- Inspired by Bloomberg BLPAPI structure
- Supports integration with Databricks / Spark 3.x
