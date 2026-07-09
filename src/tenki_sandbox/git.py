from __future__ import annotations


class SandboxGit:
    def __init__(self, sandbox) -> None:
        self._sandbox = sandbox

    def clone(self, repo: str, *, branch: str | None = None, depth: int | None = None, directory: str | None = None) -> str:
        args = {"repo": repo}
        if branch:
            args["branch"] = branch
        if depth:
            args["depth"] = str(depth)
        if directory:
            args["directory"] = directory
        return self._sandbox._git_operation("clone", args)

    def checkout(self, ref: str, *, create: bool = False) -> str:
        args = {"ref": ref}
        if create:
            args["create"] = "true"
        return self._sandbox._git_operation("checkout", args)

    def diff(
        self,
        *,
        range: str | None = None,
        base: str | None = None,
        head: str | None = None,
        path: str | None = None,
    ) -> str:
        args = {}
        if range:
            args["range"] = range
        if base:
            args["base"] = base
        if head:
            args["head"] = head
        if path:
            args["path"] = path
        return self._sandbox._git_operation("diff", args)

    def log(self, *, max_count: int | None = None, range: str | None = None, path: str | None = None) -> str:
        args = {}
        if max_count:
            args["max_count"] = str(max_count)
        if range:
            args["range"] = range
        if path:
            args["path"] = path
        return self._sandbox._git_operation("log", args)

    def fetch_pr(self, pr_number: int, *, remote: str = "origin", branch_prefix: str = "pr", directory: str | None = None) -> str:
        if pr_number <= 0:
            raise ValueError("pr_number must be > 0")
        branch = f"{branch_prefix}-{pr_number}"
        base_args = []
        if directory:
            base_args.extend(["-C", directory])
        self._sandbox.exec(
            "git",
            *base_args,
            "fetch",
            remote,
            f"pull/{pr_number}/head:{branch}",
            check=True,
        )
        return self._sandbox.exec("git", *base_args, "checkout", branch, check=True).stdout_text

