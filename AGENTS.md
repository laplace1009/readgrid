# Repository Guidelines

## Project Structure & Module Organization
This repository is currently minimal: `README.md`, `LICENSE`, and Rust-focused ignore rules in `.gitignore`. There is no committed `Cargo.toml` or `src/` tree yet.

When adding code, keep the layout conventional: `src/` for application or library code, `tests/` for integration tests, `examples/` for runnable samples, and `assets/` only for static files required by tests or docs.

## Build, Test, and Development Commands
No build pipeline is committed yet. For new Rust code, standardize on Cargo: `cargo fmt` to format source, `cargo check` for a fast compile pass, `cargo clippy --all-targets --all-features -D warnings` for linting, `cargo test` for unit and integration tests, and `cargo run` for the default binary once one exists.

If you introduce new tooling, document it in `README.md` and keep commands scriptable.

## Coding Style & Naming Conventions
Follow standard Rust style with 4-space indentation and `rustfmt` formatting. Use `snake_case` for functions, modules, and files, `PascalCase` for types and traits, and `SCREAMING_SNAKE_CASE` for constants. Keep modules focused and prefer small public APIs over wide exports. Name binaries and crates clearly, for example `readgrid-cli`.

## Testing Guidelines
Place unit tests beside the code they exercise and integration tests under `tests/`. Name test files after the behavior under test, such as `tests/grid_parser.rs`. Cover new branches and error cases, not only happy paths. Run `cargo test` locally before opening a pull request.

## Spec-First Workflow
For greenfield projects, major features, large refactors, or ambiguous requests, use `spec-interviewer` before writing production code. Do not start implementation until the spec clearly covers goals, non-goals, scope, core flows, functional requirements, non-functional requirements, constraints, acceptance criteria, and open questions or explicit decisions.

If the request is still fuzzy, interview first. Once the spec is approved, create a `/plan` before implementation. Small, well-scoped fixes can skip this workflow when acceptance criteria are already clear.

## Commit & Pull Request Guidelines
Create a branch before starting work, and make the name explicit about the change. Use prefixes such as `feat/`, `fix/`, `docs/`, or `chore/`, for example `feat/grid-parser`, `fix/input-validation`, or `docs/contributor-guide`.

History currently starts with `Initial commit`, so keep commit subjects short, imperative, and under 72 characters, for example `Add grid parser skeleton`. Separate refactors from behavior changes when practical.

Pull requests should include a short summary, testing notes, and links to any related issue. Add sample output or screenshots only when they clarify user-visible behavior.
