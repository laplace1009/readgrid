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
cargo run -- --sqlite-path sample/readgrid_demo.db --table tasks
cargo run -- --sqlite-path sample/readgrid_demo.db --table tasks --view detail
cargo run -- --profile local-postgres --schema public --table tasks --view graph
```

CLI 형식:

```text
readgrid [--profile NAME | --pg-url URL | --sqlite-path PATH] [--schema NAME] [--table NAME] [--view detail|graph] [--mcp-context-file PATH]
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
{"profile":{"name":"sample-sqlite","kind":"sqlite","path":"sample/readgrid_demo.db"},"target_schema":null,"target_table":"tasks","target_view":"detail"}
```

기존 `preferred_schema` 키도 계속 읽을 수 있으며, `target_schema`의 별칭으로 처리됩니다.

### 딥 링크 시작 동작

- `--schema`, `--table`, `--view`는 CLI에서 MCP 컨텍스트보다 우선합니다.
- `--view`를 생략하면 브라우저 화면에서 대상 테이블만 선택합니다.
- PostgreSQL에서 테이블이나 뷰를 바로 열려면 스키마가 필요합니다. 스키마가 없으면 스키마 선택 화면에서 멈추고 안내 메시지를 보여줍니다.
- SQLite에서는 스키마 힌트를 무시하고 테이블/뷰 대상으로 바로 이어갑니다.
- 잘못된 스키마나 테이블을 요청하면 가장 가까운 유효 화면으로 돌아가고 상태 줄에 이유를 표시합니다.

### 데모 데이터베이스

샘플 SQLite 데이터베이스는 `sample/readgrid_demo.db`에 들어 있습니다.

```bash
cargo run -- --sqlite-path sample/readgrid_demo.db
sqlite3 sample/readgrid_demo.db < sample/readgrid_demo.sql
```

### 기본 키 조작

- 연결 화면: `Enter` 연결, `Esc` 또는 `q` 종료
- 테이블 브라우저: `/` 필터, `r` 새로고침, `Enter` 상세 보기
- 상세 화면: `e` CSV 내보내기, `f` 필터 추가, `[` `]` 정렬 컬럼 이동, `s` 정렬 방향 전환, `n` `p` 페이지 이동, `Enter` 관계 이동, `g` 관계 그래프, `Esc` 뒤로
- CSV 내보내기는 현재 화면에 보이는 미리보기 페이지 한 장만 저장하며, 기본 경로는 `db_csv/` 아래에 제안됩니다.

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
cargo run -- --sqlite-path sample/readgrid_demo.db --table tasks
cargo run -- --sqlite-path sample/readgrid_demo.db --table tasks --view detail
cargo run -- --profile local-postgres --schema public --table tasks --view graph
```

CLI shape:

```text
readgrid [--profile NAME | --pg-url URL | --sqlite-path PATH] [--schema NAME] [--table NAME] [--view detail|graph] [--mcp-context-file PATH]
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
{"profile":{"name":"sample-sqlite","kind":"sqlite","path":"sample/readgrid_demo.db"},"target_schema":null,"target_table":"tasks","target_view":"detail"}
```

The legacy `preferred_schema` key is still accepted as an alias for `target_schema`.

### Deep-Link Startup

- `--schema`, `--table`, and `--view` from the CLI override matching MCP target fields.
- Omitting `--view` keeps ReadGrid in the browser and simply preselects the requested table.
- PostgreSQL needs a schema before it can open a specific table or view, so missing schemas fall back to the schema picker with a status message.
- SQLite ignores schema hints and continues directly to the requested table or view.
- Invalid schemas or tables fall back to the nearest valid screen and explain the reason in the status line.

### Demo Database

A small SQLite demo database is available at `sample/readgrid_demo.db`.

```bash
cargo run -- --sqlite-path sample/readgrid_demo.db
sqlite3 sample/readgrid_demo.db < sample/readgrid_demo.sql
```

### Key Controls

- Connections: `Enter` connect, `Esc` or `q` quit
- Table browser: `/` filter, `r` reload, `Enter` open detail
- Detail view: `e` export CSV, `f` add filter, `[` `]` move sort column, `s` toggle sort order, `n` `p` change page, `Enter` open relations, `g` relationship graph, `Esc` back
- CSV export saves only the currently visible preview page in v1, and the default path is suggested under `db_csv/`.

### Development

```bash
cargo fmt
cargo check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```

### Additional Docs

- `docs/subagents.md`
