# postgres_copy

A lightweight Python library to simplify high-performance interactions with PostgreSQL, including support for bulk inserts via `COPY FROM`, basic querying, and seamless integration with Pydantic models.

---

## 📦 Installation

### From GitHub

```bash
pip install git+https://github.com/justin-pfeifer/postgres-copy.git
```

---

## 🧑‍💻 Usage

### 1. Connect

```python
from postgres_copy import Postgres

pg = Postgres(
    host="localhost",
    dbname="mydb",
    user="myuser",
    password="mypassword"
)
```

### 2. Run a Query

```python
result = pg.sql_result("SELECT * FROM users LIMIT 1")
print(result)
```

### 3. Bulk Insert Data

```python
data = [
    {"id": 1, "name": "Alice"},
    {"id": 2, "name": "Bob"},
]

pg.insert("public.users", data)
```

---

## 🧵 Streaming Support (Advanced)

Use `StringIteratorIO` to stream line-by-line inserts into PostgreSQL COPY.

```python
from postgres_copy.stringiterator import StringIteratorIO

def csv_lines(data):
    yield "id,name\n"
    for row in data:
        yield f"{row['id']},{row['name']}\n"

with pg.connection.cursor() as cur:
    with cur.copy("COPY public.users FROM STDIN WITH (FORMAT csv)") as copy:
        stream = StringIteratorIO(csv_lines(data))
        while chunk := stream.read(1024):
            copy.write(chunk)
```

---

## 📁 Project Structure

```
postgres_copy/
├── postgres.py         # Core wrapper
├── stringiterator.py   # Streaming support for COPY
├── itertest.py         # (Optional) iterable test class
```

---

## 📜 License

MIT
