# Repository Guidelines

## Project Structure & Module Organization
- `packages/` is a Yarn workspace of JS/TS packages; start with `cubejs-server`, `cubejs-schema-compiler`, and drivers under `cubejs-*-driver` when extending backend behavior.
- `rust/` hosts the Cubestore and orchestration crates; use Cargo workflows there and surface bindings through the co-located `js-wrapper` packages.
- Reference assets and docs in `docs/` and runnable examples in `examples/`; `dev-env.sh` and `.nvmrc` (Node 22.20.0) keep local environments aligned.

## Build, Test, and Development Commands
- `yarn install --frozen-lockfile` installs all workspace dependencies using the Yarn+Lerna layout.
- `yarn build` packages client/core bundles (`lerna run build:client-core` + `rollup -c`); `yarn watch` (or `watch-local`) keeps bundles hot-reloaded.
- `yarn tsc` / `yarn tsc:watch` type-check shared TS sources; `yarn lint` and `yarn lint:fix` enforce the monorepo ESLint rules.
- Run tests via `yarn lerna run test --stream`, or target a package (`yarn workspace @cubejs-backend/server test`) for focused work.

## Coding Style & Naming Conventions
- `.editorconfig` enforces 2-space indentation for JS/TS and 4 spaces for Rust; always commit LF endings.
- ESLint config comes from `packages/cubejs-linter` (Airbnb base + TypeScript rules, single quotes, no semicolons); rely on lint autofixes before pushing.
- Prefer PascalCase for classes, camelCase for functions/variables, and kebab-case for folders mirroring package names; keep shared types in `src` and declaration files under `dist/src`.

## Testing Guidelines
- Jest drives JS/TS unit tests (`*.test.ts|js` under each package’s `test/` folder); many drivers expose `unit` and `integration` scripts—run both when touching adapters.
- Integration and end-to-end suites live in `packages/cubejs-testing*`; use `.env` fixtures or Docker services defined alongside the tests.
- Codecov tracks coverage (`codecov.yml` baseline 70%); surface new behavior with additional tests and include snapshots updates via `jest --updateSnapshot` when needed.

## Commit & Pull Request Guidelines
- Follow Conventional Commits observed in history (`feat(api-gateway): …`, `chore(ci): …`) and include scope when touching a specific package.
- Every commit must be signed (`git commit -s`) to satisfy the repository’s DCO check.
- Pull requests should summarize intent, link issues, list new commands or env vars, attach relevant screenshots/log excerpts, and confirm `yarn lint` + targeted tests in the description.
