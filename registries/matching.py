import re
from typing import Any, Dict, List, Optional, Sequence, Tuple

from rapidfuzz import fuzz, process

from pipelineFramework import normalize_key

# Values the source databases use instead of leaving a field empty.
PLACEHOLDERS = {
    "-",
    "--",
    "---",
    ".",
    "..",
    "/",
    "?",
    "n/a",
    "n.a.",
    "na",
    "k.a.",
    "ka",
    "keine angabe",
    "keine",
    "unbekannt",
    "unknown",
    "none",
    "null",
    "nan",
    "nil",
    "tbd",
    "tba",
    "xxx",
}

TRANSLITERATIONS = {
    "ä": "ae",
    "ö": "oe",
    "ü": "ue",
    "ß": "ss",
    "æ": "ae",
    "ø": "oe",
    "å": "aa",
    "á": "a",
    "à": "a",
    "â": "a",
    "ã": "a",
    "é": "e",
    "è": "e",
    "ê": "e",
    "ë": "e",
    "í": "i",
    "ì": "i",
    "î": "i",
    "ï": "i",
    "ó": "o",
    "ò": "o",
    "ô": "o",
    "õ": "o",
    "ú": "u",
    "ù": "u",
    "û": "u",
    "ç": "c",
    "ñ": "n",
    "š": "s",
    "ž": "z",
    "č": "c",
    "ć": "c",
    "đ": "d",
    "ł": "l",
    "ő": "o",
    "ű": "u",
    "ș": "s",
    "ț": "t",
    "ı": "i",
}

LEGAL_FORM_TOKENS = {
    "gmbh",
    "mbh",
    "gesmbh",
    "ges",
    "ag",
    "kg",
    "kgaa",
    "og",
    "ohg",
    "keg",
    "gesbr",
    "se",
    "privatstiftung",
    "egen",
    "gen",
    "reg",
    "ltd",
    "limited",
    "llc",
    "lp",
    "inc",
    "incorporated",
    "plc",
    "bv",
    "nv",
    "sarl",
    "sas",
    "srl",
    "spa",
    "oy",
    "oyj",
    "aps",
    "corp",
    "corporation",
}
TRAILING_NOISE = {"&", "und", "co", "cie"}

_COUNTRY_ALIASES: Dict[str, Tuple[str, ...]] = {
    "AT": ("österreich", "oesterreich", "austria", "aut"),
    "DE": ("deutschland", "germany", "deu", "ger", "brd"),
    "CH": ("schweiz", "switzerland", "suisse", "che"),
    "LI": ("liechtenstein", "lie"),
    "IT": ("italien", "italy", "italia", "ita"),
    "FR": ("frankreich", "france", "fra"),
    "GB": (
        "vereinigtes königreich",
        "vereinigtes koenigreich",
        "grossbritannien",
        "großbritannien",
        "united kingdom",
        "great britain",
        "england",
        "schottland",
        "scotland",
        "gbr",
        "uk",
    ),
    "IE": ("irland", "ireland", "irl"),
    "US": ("vereinigte staaten von amerika", "vereinigte staaten", "usa", "united states", "united states of america"),
    "CA": ("kanada", "canada", "can"),
    "ES": ("spanien", "spain", "españa", "espana", "esp"),
    "PT": ("portugal", "prt"),
    "NL": ("niederlande", "netherlands", "nld", "holland"),
    "BE": ("belgien", "belgium", "bel"),
    "LU": ("luxemburg", "luxembourg", "lux"),
    "DK": ("dänemark", "daenemark", "denmark", "dnk"),
    "SE": ("schweden", "sweden", "swe"),
    "NO": ("norwegen", "norway", "nor"),
    "FI": ("finnland", "finland", "fin"),
    "IS": ("island", "iceland", "isl"),
    "PL": ("polen", "poland", "pol"),
    "CZ": ("tschechien", "tschechische republik", "czech republic", "czechia", "cze"),
    "SK": ("slowakei", "slovakia", "svk"),
    "HU": ("ungarn", "hungary", "hun"),
    "SI": ("slowenien", "slovenia", "svn"),
    "HR": ("kroatien", "croatia", "hrv"),
    "RS": ("serbien", "serbia", "srb"),
    "BA": ("bosnien und herzegowina", "bosnia and herzegovina", "bih"),
    "ME": ("montenegro", "mne"),
    "MK": ("nordmazedonien", "north macedonia", "mkd"),
    "AL": ("albanien", "albania", "alb"),
    "BG": ("bulgarien", "bulgaria", "bgr"),
    "RO": ("rumänien", "rumaenien", "romania", "rou"),
    "GR": ("griechenland", "greece", "grc"),
    "TR": ("türkei", "tuerkei", "turkey", "türkiye", "tur"),
    "UA": ("ukraine", "ukr"),
    "RU": ("russland", "russische föderation", "russia", "rus"),
    "BY": ("belarus", "weissrussland", "weißrussland", "blr"),
    "LT": ("litauen", "lithuania", "ltu"),
    "LV": ("lettland", "latvia", "lva"),
    "EE": ("estland", "estonia", "est"),
    "CY": ("zypern", "cyprus", "cyp"),
    "MT": ("malta", "mlt"),
    "IL": ("israel", "isr"),
    "JP": ("japan", "jpn"),
    "CN": ("china", "volksrepublik china", "chn"),
    "KR": ("südkorea", "suedkorea", "korea", "republik korea", "south korea", "kor"),
    "TW": ("taiwan", "twn"),
    "IN": ("indien", "india", "ind"),
    "AU": ("australien", "australia", "aus"),
    "NZ": ("neuseeland", "new zealand", "nzl"),
    "BR": ("brasilien", "brazil", "bra"),
    "AR": ("argentinien", "argentina", "arg"),
    "CL": ("chile", "chl"),
    "MX": ("mexiko", "mexico", "mex"),
    "ZA": ("südafrika", "suedafrika", "south africa", "zaf"),
    "EG": ("ägypten", "aegypten", "egypt", "egy"),
    "MA": ("marokko", "morocco", "mar"),
    "TN": ("tunesien", "tunisia", "tun"),
    "SG": ("singapur", "singapore", "sgp"),
    "TH": ("thailand", "tha"),
    "VN": ("vietnam", "vnm"),
    "ID": ("indonesien", "indonesia", "idn"),
    "SA": ("saudi-arabien", "saudi arabien", "saudi arabia", "sau"),
    "AE": ("vereinigte arabische emirate", "united arab emirates", "are"),
    "QA": ("katar", "qatar", "qat"),
    "KZ": ("kasachstan", "kazakhstan", "kaz"),
    "GE": ("georgien", "georgia", "geo"),
    "AM": ("armenien", "armenia", "arm"),
}
COUNTRY_CODES = {alias: code for code, aliases in _COUNTRY_ALIASES.items() for alias in (*aliases, code.lower())}

# The reprojected BEV coordinates and every Austrian geocode must fall inside this box.
AUSTRIA_BBOX = (46.3, 49.1, 9.4, 17.2)

# GLEIF stores the Bundesland as its ISO 3166-2 code, which no dashboard wants to display.
AUSTRIAN_STATES = {
    "AT-1": "Burgenland",
    "AT-2": "Kärnten",
    "AT-3": "Niederösterreich",
    "AT-4": "Oberösterreich",
    "AT-5": "Salzburg",
    "AT-6": "Steiermark",
    "AT-7": "Tirol",
    "AT-8": "Vorarlberg",
    "AT-9": "Wien",
}

_NON_ALNUM = re.compile(r"[^0-9a-z]+")
_PARENTHESISED = re.compile(r"\(([^)]{2,40})\)")
_DOMAIN = re.compile(r"^[a-z0-9][a-z0-9.-]*\.[a-z]{2,24}$")
_STREET_WITH_NUMBER = re.compile(r"^(?P<street>.*?[^\d\s])[\s,]+(?P<number>\d[\d\s/a-zA-Z.\-]*)$")
_POSTAL_CODE = re.compile(r"^\d{4}$")
_CITY_SEPARATORS = re.compile(r"\s*[,;:]\s*|\s+-\s+")


def clean(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = " ".join(str(value).split())
    if not text or text.lower() in PLACEHOLDERS:
        return None
    return text


def fold(value: str) -> str:
    lowered = "".join(TRANSLITERATIONS.get(character, character) for character in value.lower())
    return " ".join(_NON_ALNUM.sub(" ", lowered).split())


def transliterate(value: str) -> str:
    return "".join(TRANSLITERATIONS.get(character, character) for character in value.lower())


def strip_legal_forms(name: str) -> str:
    parts = normalize_key(name).split()
    while parts and (fold(parts[-1]) in LEGAL_FORM_TOKENS or parts[-1] in TRAILING_NOISE):
        parts.pop()
    return " ".join(parts)


def acronyms(name: str) -> List[str]:
    return [match.strip() for match in _PARENTHESISED.findall(name)]


def name_keys(name: str) -> List[str]:
    # Keys are normalize_key'd, never folded: they are matched against the search_names /
    # search_name fields the registry loaders wrote with the same function.
    keys = {normalize_key(name)}
    stripped = strip_legal_forms(name)
    if stripped:
        keys.add(stripped)
    without_parentheses = normalize_key(_PARENTHESISED.sub(" ", name))
    if without_parentheses:
        keys.add(without_parentheses)
        keys.add(strip_legal_forms(without_parentheses))
    for acronym in acronyms(name):
        keys.add(normalize_key(acronym))
    for key in list(keys):
        keys.add(transliterate(key))
    return sorted(key for key in keys if len(key) >= 3)


def fold_names(*names: Optional[str]) -> List[str]:
    folded = {fold(name) for name in names if name}
    return sorted(value for value in folded if len(value) >= 3)


def domain_of(website: Any) -> Optional[str]:
    value = clean(website)
    if value is None:
        return None
    value = value.lower()
    value = re.sub(r"^[a-z][a-z0-9+.-]*://", "", value)
    value = value.split("/")[0].split("?")[0].split("#")[0].split("@")[-1].split(":")[0]
    if value.startswith("www."):
        value = value[4:]
    # Rejects the country codes and free text the source databases sometimes put in this column.
    return value if _DOMAIN.match(value) else None


def state_name(value: Any) -> Optional[str]:
    text = clean(value)
    return AUSTRIAN_STATES.get((text or "").upper(), text)


def country_code_of(value: Any) -> Optional[str]:
    text = clean(value)
    if text is None:
        return None
    return COUNTRY_CODES.get(text.lower())


def city_keys(city: Any) -> List[str]:
    value = clean(city)
    if value is None:
        return []
    keys = [value]
    # FFG cities arrive as "Graz,06.Bez.:Jakomini"; the part in front stays usable as a weaker key.
    head = _CITY_SEPARATORS.split(value)[0].strip()
    if head and head != value:
        keys.append(head)
    return keys


def postal_code_of(value: Any) -> Optional[str]:
    text = clean(value)
    if text is None:
        return None
    candidate = text.split()[0].strip()
    return candidate if _POSTAL_CODE.match(candidate) else None


def split_street(street: Any) -> Tuple[Optional[str], Optional[str]]:
    value = clean(street)
    if value is None:
        return None, None
    match = _STREET_WITH_NUMBER.match(value)
    if not match:
        return value, None
    return match.group("street").strip(" ,"), " ".join(match.group("number").split())


def looks_like_street(value: Any) -> bool:
    street, number = split_street(value)
    return bool(street) and any(character.isalpha() for character in street or "")


def best_match(
    queries: Sequence[str],
    candidates: Sequence[Tuple[str, str, Any]],
    threshold: float,
    margin: float,
) -> Optional[Tuple[Any, float]]:
    # candidates are (folded text, identity, payload); several texts may belong to one payload,
    # so the runner-up guard compares identities rather than positions.
    if not queries or not candidates:
        return None

    texts = [text for text, _, _ in candidates]
    scores: Dict[str, Tuple[float, int]] = {}
    for query in queries:
        for _, score, index in process.extract(
            query,
            texts,
            scorer=fuzz.token_sort_ratio,
            limit=10,
            score_cutoff=max(threshold - margin, 0),
        ):
            identity = candidates[index][1]
            if score > scores.get(identity, (0.0, 0))[0]:
                scores[identity] = (score, index)

    if not scores:
        return None

    ranked = sorted(scores.items(), key=lambda item: item[1][0], reverse=True)
    identity, (best_score, best_index) = ranked[0]
    if best_score < threshold:
        return None
    runner_up = ranked[1][1][0] if len(ranked) > 1 else 0.0
    if best_score - runner_up < margin:
        return None
    return candidates[best_index][2], float(best_score)


def in_austria(latitude: float, longitude: float) -> bool:
    south, north, west, east = AUSTRIA_BBOX
    return south <= latitude <= north and west <= longitude <= east


def is_valid_coordinate(latitude: Any, longitude: Any) -> bool:
    try:
        latitude = float(latitude)
        longitude = float(longitude)
    except TypeError, ValueError:
        return False
    if latitude != latitude or longitude != longitude:
        return False
    return -90 <= latitude <= 90 and -180 <= longitude <= 180 and not (latitude == 0 and longitude == 0)


def point(latitude: float, longitude: float) -> Dict[str, Any]:
    return {"type": "Point", "coordinates": [float(longitude), float(latitude)]}
