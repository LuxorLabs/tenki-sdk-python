from .errors import PaginationStalledError


def advance_page_token(current: str, next_token: str) -> str:
    normalized = next_token.strip()
    if normalized and normalized == current:
        raise PaginationStalledError("sandbox: pagination token did not advance")
    return normalized
