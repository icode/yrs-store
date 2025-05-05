# Yrs backend for persistent simple stores

Inspiration comes from [pycrdt-store](https://github.com/y-crdt/pycrdt-store) and [yrs-persistence](https://github.com/y-crdt/yrs-persistence)

This repository contains code of 2 crates:

- `yrs-store`: a generic library that adds a bunch of utility functions that simplify process of persisting and managing Yrs/Yjs document contents. Since it's generic, it's capabilities can be applied to basically any modern persistent simple store.
- `yrs-postgres`: an [PostgreSQL](https://www.postgresql.org/) implementation of `yrs-store`.