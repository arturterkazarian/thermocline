# Contributing to Thermocline

This document describes the project's development conventions. They apply to
every change, including changes by maintainers.

## Workflow

- **Branches**: `<type>/<short-slug>` in kebab-case, where `<type>` matches the
  Conventional Commits type of the work: `feat/cache-source`,
  `fix/hash-determinism`, `chore/bump-python-311`, `docs/readme-quickstart`.
- **Commits**: [Conventional Commits](https://www.conventionalcommits.org/) —
  `feat:`, `fix:`, `docs:`, `test:`, `refactor:`, `perf:`, `chore:`, `ci:`,
  `build:`. Imperative mood, lowercase, no trailing period:
  `feat: add CacheSource contract`. One commit = one logical change that
  leaves the project green (code and its tests land together).
- **Pull requests**: title in the same style as a commit; the description
  states what changed and why. Trivial infrastructure may go straight to
  `main`; substantive code goes through a branch and PR.
- **Language**: everything in the repository — code, docstrings, comments,
  commits, PRs — is written in English.

## Naming

- **Modules**: short singular nouns — `source.py`, `cache.py`, `eviction.py`;
  adapters live in `adapters/<backend>.py`.
- **Classes** (CapWords, by role):

  | Role                | Pattern            | Examples                             |
  |---------------------|--------------------|--------------------------------------|
  | Abstract core       | noun               | `CacheSource`                        |
  | Capability protocol | `Supports<X>`      | `SupportsHashProbe`, `SupportsDelta` |
  | Data record         | noun               | `SyncBatch`                          |
  | Strategy / policy   | `<Kind><Role>`     | `LruEviction`, `MsgpackSerializer`   |
  | Exception           | `<X>Error`         | base class `ThermoclineError`        |

- **Methods** (snake_case, by prefix):
  - `get_*` — point lookup by key, async (`get`, `get_hash`);
  - `load_*` — bulk or streaming retrieval (`load_all`, `load_changed`);
  - `*_of(obj)` — cheap synchronous extractor from an object already in hand
    (`key_of`, `hash_of`); no `compute_`/`calc_` prefixes;
  - booleans start with `is_` / `has_` / `supports_`;
  - internal names take a single leading underscore.
- **Type variables**: `K` for keys, `T` for objects, project-wide; variance
  variants carry the standard `_co` / `_contra` suffixes.
- **Constants**: `UPPER_SNAKE_CASE`.

## Docstrings

Google style ([enforced by ruff](#tooling)) with reST cross-references:

- Every public module, class, and method has a docstring. The first line is a
  single imperative sentence ending with a period: `Stream every live object.`
- Sections: `Args:`, `Returns:`, `Raises:`, `Attributes:`. Constructor
  arguments are documented in the class docstring, not on `__init__`.
- Cross-reference with `` :class:`X` `` / `` :meth:`X.y` ``; put literals in
  ``double backticks``.
- Design rationale — the *why* — lives in module docstrings, not in comments.
- Obligations for implementers of an interface go in a bulleted list under
  `Contract for implementations:` in the method docstring.
- Test functions carry no docstrings; the name must speak for itself:
  `test_<unit>_<behavior>`.
- Inline comments state only non-obvious constraints the code cannot express.

## Tooling

All checks must pass before a commit:

```bash
uv run pytest        # tests
uv run mypy src      # types (strict)
uv run ruff check .  # lint, incl. docstring conventions (pydocstyle/google)
uv run ruff format --check .
```

The project targets the oldest supported Python (see `.python-version`);
`requires-python` and the CI matrix define the full range. When a Python
version reaches end of life, support for it is removed.
