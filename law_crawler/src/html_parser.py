import re
from urllib.parse import urlparse, parse_qs
from bs4 import BeautifulSoup

class HTMLParserUtils:
    @staticmethod
    def extract_metadata(soup, url):
        title = soup.title.string.strip() if soup.title else "Untitled"
        parsed = urlparse(url)
        act_id = parse_qs(parsed.query).get("actid", [""])[0].replace("%20", " ")
        cap_match = re.search(r"CAP\.?\s?\d+[A-Z]?", title, re.IGNORECASE)
        year_match = re.search(r"\b(19|20)\d{2}\b", title)
        return {
            "title": title,
            "act_id": act_id or None,
            "cap": cap_match.group(0) if cap_match else None,
            "year": int(year_match.group(0)) if year_match else None
        }

    @staticmethod
    def extract_sections(soup):
        sections = []
        current_section = {}
        for tag in soup.select("h1, h2, h3, p"):
            text = tag.get_text(strip=True)
            match = re.match(r"^(Section\s*)?(\d+[A-Za-z]?)[\.\-–\)]?\s*(.+)?", text, re.IGNORECASE)
            if match:
                if current_section:
                    sections.append(current_section)
                current_section = {
                    "section_number": match.group(2).strip(),
                    "title": (match.group(3) or "").strip(),
                    "text": ""
                }
            elif current_section:
                current_section["text"] += "\n" + text.strip()

        if current_section:
            sections.append(current_section)

        return sections

    @staticmethod
    def extract_act_body(html, title="Act"):
        soup = BeautifulSoup(html, "html.parser")
        selectors = ["div.akn", "div#akn", "div.akn-body", "div#akn-body", "div.akn-document"]
        for sel in selectors:
            body = soup.select_one(sel)
            if body:
                break
        else:
            body = soup.find("main") or soup.body

        return f"""<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8" /><title>{title}</title></head><body>{body.prettify() if body else '<p>[No content found]</p>'}</body></html>"""

    @staticmethod
    def convert_to_markdown(soup, title="Document"):
        lines = [f"# {title}", ""]
        for tag in soup.body.descendants:
            if tag.name in ["h1", "h2", "h3"]:
                lines.append(f"{'#' * int(tag.name[1])} {tag.get_text(strip=True)}\n")
            elif tag.name == "p":
                text = tag.get_text(strip=True)
                if text:
                    lines.append(f"{text}\n")
            elif tag.name == "ul":
                lines.extend(f"- {li.get_text(strip=True)}" for li in tag.find_all("li"))
            elif tag.name == "ol":
                lines.extend(f"{i}. {li.get_text(strip=True)}" for i, li in enumerate(tag.find_all("li"), 1))
            elif tag.name == "br":
                lines.append("")
        return "\n".join(lines)
