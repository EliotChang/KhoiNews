from __future__ import annotations

import re


LITERAL_URL_PATTERN = re.compile(
    r"(?i)\b(?:https?://|www\.)\S+\b|"
    r"(?<!@)\b"
    r"(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+"
    r"[a-z]{2,63}\b"
    r"(?::\d{2,5})?"
    r"(?:/\S*)?"
)
LITERAL_EMAIL_PATTERN = re.compile(
    r"(?i)\b[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}\b"
)
OBFUSCATED_DOMAIN_PATTERN = re.compile(
    r"(?ix)"
    r"\b[a-z0-9][a-z0-9-]{0,62}"
    r"(?:\s*(?:\(\s*dot\s*\)|\[\s*dot\s*\]|\{\s*dot\s*\}|\s+dot\s+)\s*[a-z0-9-]{1,63}){1,4}"
    r"(?:\s*(?:/|\s+slash\s+)\s*[a-z0-9\-._~:/?#\[\]@!$&'()*+,;=%\s]*)?"
)
OBFUSCATED_EMAIL_PATTERN = re.compile(
    r"(?ix)"
    r"\b[a-z0-9._%+-]+"
    r"\s*(?:@|\(\s*at\s*\)|\[\s*at\s*\]|\{\s*at\s*\}|\s+at\s+)\s*"
    r"[a-z0-9-]+"
    r"(?:\s*(?:\(\s*dot\s*\)|\[\s*dot\s*\]|\{\s*dot\s*\}|\s+dot\s+)\s*[a-z0-9-]+){1,4}\b"
)
URL_LIKE_PATTERNS = (
    LITERAL_URL_PATTERN,
    LITERAL_EMAIL_PATTERN,
    OBFUSCATED_DOMAIN_PATTERN,
    OBFUSCATED_EMAIL_PATTERN,
)


def strip_urls_from_text(value: str) -> str:
    if not value:
        return ""
    without_urls = value
    for pattern in URL_LIKE_PATTERNS:
        without_urls = pattern.sub(" ", without_urls)
    # Remove residual obfuscated "at" markers that may remain after partial URL/email stripping.
    without_urls = re.sub(r"(?i)\(\s*at\s*\)|\[\s*at\s*\]|\{\s*at\s*\}", " ", without_urls)
    # Remove isolated path separators left behind by URL-only inputs.
    without_urls = re.sub(r"(^|\\s)[/\\\\]+(?=\\s|$)", " ", without_urls)
    without_url_punctuation = re.sub(r"\s+([,.;:!?，。、；：！？])", r"\1", without_urls)
    normalized = re.sub(r"\s+", " ", without_url_punctuation).strip()
    if normalized in {"/", "\\", "|", "-", "_", ":", ".", ".."}:
        return ""
    return normalized


def contains_url_text(value: str) -> bool:
    if not value:
        return False
    return any(pattern.search(value) for pattern in URL_LIKE_PATTERNS)
