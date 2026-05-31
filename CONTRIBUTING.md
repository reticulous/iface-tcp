# Contributing

All contributions to the spangap / reticulous projects must be made under
the **Developer Certificate of Origin (DCO) 1.1**. Every commit message
must end with a `Signed-off-by:` trailer carrying your real name and a
reachable email address:

    Signed-off-by: Your Name <your@email>

Add it on each commit by passing `-s` (or `--signoff`) to `git commit`:

    git commit -s -m "Your commit message"

The DCO 1.1 text follows. Signing off on a commit means you are agreeing
to this:

```
Developer Certificate of Origin
Version 1.1

Copyright (C) 2004, 2006 The Linux Foundation and its contributors.

Everyone is permitted to copy and distribute verbatim copies of this
license document, but changing it is not allowed.


Developer's Certificate of Origin 1.1

By making a contribution to this project, I certify that:

(a) The contribution was created in whole or in part by me and I
    have the right to submit it under the open source license
    indicated in the file; or

(b) The contribution is based upon previous work that, to the best
    of my knowledge, is covered under an appropriate open source
    license and I have the right under that license to submit that
    work with modifications, whether created in whole or in part
    by me, under the same open source license (unless I am
    permitted to submit under a different license), as indicated
    in the file; or

(c) The contribution was provided directly to me by some other
    person who certified (a), (b) or (c) and I have not modified
    it.

(d) I understand and agree that this project and the contribution
    are public and that a record of the contribution (including all
    personal information I submit with it, including my sign-off) is
    maintained indefinitely and may be redistributed consistent with
    this project or the open source license(s) involved.
```

The canonical source is <https://developercertificate.org/>.

A GitHub Action enforces this on every push: commits missing a valid
`Signed-off-by:` trailer will fail the DCO check and need to be amended
or rebased with sign-offs before they're accepted.

---

## Optional: auto-sign-off locally

If you'd rather not type `-s` on every commit, this repository ships an
optional `prepare-commit-msg` hook at `.githooks/prepare-commit-msg` that
auto-appends the trailer using your `user.name` / `user.email` git
config. To enable it for this clone:

    git config core.hooksPath .githooks

That's a one-time setup per clone. From then on, `git commit` (with or
without `-s`) will produce a signed-off commit automatically.

The hook is local-only and opt-in; declining it just means you go on
typing `-s` (or `git commit --amend --signoff` when you forget).
