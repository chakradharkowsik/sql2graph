import re

def sanitize_page_field(s: str, maxlen: int = 200) -> str:
    """
    Clean pageTitle / pageUrl values that may be wrapped in
    <WebsiteContent_<id>>...</WebsiteContent_<id>> tags or other HTML-like tags.
    Returns the inner text (if present), with tags removed, whitespace collapsed,
    and result truncated to maxlen.
    """
    if s is None:
        return ""
    s = str(s)

    # 1) Extract inner text for WebsiteContent wrappers (non-greedy)
    #    e.g. "<WebsiteContent_M...>inner</WebsiteContent_M...>" -> "inner"
    s = re.sub(r'<WebsiteContent_[^>]*>(.*?)</WebsiteContent_[^>]*>', r'\1', s, flags=re.DOTALL|re.IGNORECASE)

    # 2) Remove any remaining single tags like <WebsiteContent_...> or </WebsiteContent_...>
    s = re.sub(r'</?WebsiteContent_[^>]*>', '', s, flags=re.IGNORECASE)

    # 3) Remove any other angle-bracket tags (generic HTML/XML)
    s = re.sub(r'<[^>]+>', '', s)

    # 4) Collapse whitespace and trim
    s = re.sub(r'\s+', ' ', s).strip()

    # 5) Optional: if the result is just a URL or empty, return empty to avoid using it as a table name
    low = s.lower()
    if not s or low.startswith('http://') or low.startswith('https://'):
        return ""

    # 6) Truncate to safe length
    return s[:maxlen]
