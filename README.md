# readgrid

<p align="center">
  <a href="#korean"><strong>한국어</strong></a> |
  <a href="#english"><strong>English</strong></a>
</p>

> GitHub README does not support native tabs, so the links above jump between language sections.

## Korean

ReadGrid는 PostgreSQL과 SQLite를 터미널에서 탐색하기 위한 TUI 데이터베이스 브라우저입니다. MCP 컨텍스트를 통해 미리 선택된 연결 정보를 받아 바로 탐색 흐름으로 넘길 수 있습니다.

### 주요 기능

- PostgreSQL과 SQLite를 하나의 TUI에서 탐색
- 스키마, 테이블, 컬럼, 인덱스, 외래 키 확인
- 데이터 미리보기, 페이지 이동, 정렬, 필터링
- 관계 그래프 탐색과 drill-through 이동
- CLI 인자, 저장된 프로필, MCP 컨텍스트로 연결 선택

### 빠른 시작

```bash
cargo run -- --sqlite-path sample/readgrid_demo.db
cargo run -- --pg-url postgres://localhost/app
cargo run -- --profile local-postgres
```

CLI 형식:

```text
readgrid [--profile NAME] [--pg-url URL] [--sqlite-path PATH] [--schema NAME] [--mcp-context-file PATH]
```

### 프로필 설정

ReadGrid는 OS 기본 설정 디렉터리 아래에서 프로필을 읽습니다. Linux에서는 보통 `~/.config/readgrid/profiles.toml` 경로를 사용합니다.

```toml
[[profiles]]
name = "local-postgres"
kind = "postgres"
url = "postgres://localhost/app"

[[profiles]]
name = "sample-sqlite"
kind = "sqlite"
path = "./sample.db"
```

### MCP 컨텍스트 전달

다음 세 가지 방법으로 미리 선택된 연결 정보를 넘길 수 있습니다.

- `--mcp-context-file <PATH>`
- `READGRID_MCP_CONTEXT_FILE`
- `READGRID_MCP_CONTEXT`

예시 JSON:

```json
{"profile":{"name":"sample-sqlite","kind":"sqlite","path":"sample/readgrid_demo.db"},"preferred_schema":null}
```

### 데모 데이터베이스

샘플 SQLite 데이터베이스는 `sample/readgrid_demo.db`에 들어 있습니다.

```bash
cargo run -- --sqlite-path sample/readgrid_demo.db
sqlite3 sample/readgrid_demo.db < sample/readgrid_demo.sql
```

### 기본 키 조작

- 연결 화면: `Enter` 연결, `Esc` 또는 `q` 종료
- 테이블 브라우저: `/` 필터, `r` 새로고침, `Enter` 상세 보기
- 상세 화면: `f` 필터 추가, `[` `]` 정렬 컬럼 이동, `s` 정렬 방향 전환, `n` `p` 페이지 이동, `Enter` 관계 이동, `g` 관계 그래프, `Esc` 뒤로

### 개발

```bash
cargo fmt
cargo check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```

### 추가 문서

- `docs/subagents.md`

## English

ReadGrid is a terminal-first TUI database explorer for PostgreSQL and SQLite. It also supports a lightweight MCP context handoff path so another tool can preselect a connection before launching the explorer.

### Highlights

- Explore PostgreSQL and SQLite from one TUI
- Browse schemas, tables, columns, indexes, and foreign keys
- Preview data with pagination, sorting, and filters
- Traverse relationship graphs and drill through linked rows
- Select connections from CLI flags, saved profiles, or MCP context

### Quick Start

```bash
cargo run -- --sqlite-path sample/readgrid_demo.db
cargo run -- --pg-url postgres://localhost/app
cargo run -- --profile local-postgres
```

CLI shape:

```text
readgrid [--profile NAME] [--pg-url URL] [--sqlite-path PATH] [--schema NAME] [--mcp-context-file PATH]
```

### Profiles

ReadGrid loads profiles from your OS config directory. On Linux, the default path is typically `~/.config/readgrid/profiles.toml`.

```toml
[[profiles]]
name = "local-postgres"
kind = "postgres"
url = "postgres://localhost/app"

[[profiles]]
name = "sample-sqlite"
kind = "sqlite"
path = "./sample.db"
```

### MCP Context Handoff

You can provide a preselected connection with any of the following:

- `--mcp-context-file <PATH>`
- `READGRID_MCP_CONTEXT_FILE`
- `READGRID_MCP_CONTEXT`

Example JSON:

```json
{"profile":{"name":"sample-sqlite","kind":"sqlite","path":"sample/readgrid_demo.db"},"preferred_schema":null}
```

### Demo Database

A small SQLite demo database is available at `sample/readgrid_demo.db`.

```bash
cargo run -- --sqlite-path sample/readgrid_demo.db
sqlite3 sample/readgrid_demo.db < sample/readgrid_demo.sql
```

### Key Controls

- Connections: `Enter` connect, `Esc` or `q` quit
- Table browser: `/` filter, `r` reload, `Enter` open detail
- Detail view: `f` add filter, `[` `]` move sort column, `s` toggle sort order, `n` `p` change page, `Enter` open relations, `g` relationship graph, `Esc` back

### Development

```bash
cargo fmt
cargo check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```

### Additional Docs

- `docs/subagents.md`
