# readgrid

## Demo database

A small SQLite demo database is available at `sample/readgrid_demo.db`.

Run the TUI against it with:

```bash
cargo run -- --sqlite-path sample/readgrid_demo.db
```

Rebuild the demo database from SQL with:

```bash
sqlite3 sample/readgrid_demo.db < sample/readgrid_demo.sql
```
