import pytest

from tenki_sandbox import PaginationStalledError
from tenki_sandbox._pagination import advance_page_token


def test_advance_page_token_normalizes_and_rejects_repeated_tokens():
    assert advance_page_token("", " next ") == "next"
    with pytest.raises(PaginationStalledError, match="pagination token did not advance"):
        advance_page_token("next", " next ")
