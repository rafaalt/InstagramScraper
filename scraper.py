import os
import csv
import time
import json
import logging
from typing import Optional, Dict, Any, List

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

class RateLimiter:
    def __init__(self, rate: int, per_seconds: int):
        self.rate = rate
        self.per = per_seconds
        self.allowance = rate
        self.last_check = time.monotonic()

    def wait(self):
        now = time.monotonic()
        elapsed = now - self.last_check
        self.last_check = now
        self.allowance = min(self.rate, self.allowance + elapsed * (self.rate / self.per))
        if self.allowance < 1.0:
            sleep_for = (1.0 - self.allowance) * (self.per / self.rate)
            time.sleep(max(sleep_for, 0))
            self.allowance = 0
        else:
            self.allowance -= 1.0

class InstagramPublicScraper:
    BASE = "https://www.instagram.com/api/v1/users/web_profile_info/"

    def __init__(
        self,
        max_retries: int = 5,
        backoff_factor: float = 1.0,
        per_host_rate: int = 6,      # requisições por janela
        per_host_window: int = 60,   # janela (segundos)
        timeout: float = 20.0,
        user_agent: str = "Mozilla/5.0",
        ig_app_id: str = "936619743392459",
        use_requests_cache: bool = True,
        sessionid_cookie: Optional[str] = None,
    ):
        self.timeout = timeout
        self.rate_limiter = RateLimiter(per_host_rate, per_host_window)
        self.session = requests.Session()

        retry = Retry(
            total=max_retries,
            connect=max_retries,
            read=max_retries,
            status=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD", "OPTIONS"],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

        self.session.headers.update({
            "User-Agent": user_agent,
            "Accept": "application/json",
            "X-IG-App-ID": ig_app_id,
        })

        # Cookie de sessão (opcional, ajuda a reduzir 401/429)
        if sessionid_cookie:
            self.session.cookies.update({"sessionid": sessionid_cookie})

        if use_requests_cache:
            try:
                import requests_cache
                requests_cache.install_cache("ig_cache", backend="sqlite", expire_after=120)
                logging.info("Cache habilitado (requests-cache, expira em 120s).")
            except Exception as e:
                logging.warning(f"Não consegui habilitar cache: {e}")

    def _maybe_wait_retry_after(self, resp: requests.Response):
        ra = resp.headers.get("Retry-After")
        if ra:
            try:
                secs = float(ra)
                sleep_for = min(max(secs, 1.0), 60.0)
                logging.warning(f"Retry-After: aguardando {sleep_for:.1f}s…")
                time.sleep(sleep_for)
            except ValueError:
                pass

    def get_profile(self, username: str) -> Optional[Dict[str, Any]]:
        """
        Retorna dict com dados básicos do perfil público, ou None se falhar.
        """
        self.rate_limiter.wait()
        try:
            resp = self.session.get(self.BASE, params={"username": username}, timeout=self.timeout)
    
            if resp.status_code == 429:
                logging.warning(f"[{username}] 429 Too Many Requests")
                self._maybe_wait_retry_after(resp)
                return None  # deixe para tentar mais tarde

            if resp.status_code == 401:
                logging.warning(f"[{username}] 401 Unauthorized (pode exigir cookie de sessão).")
                return None

            if resp.status_code == 404:
                logging.warning(f"[{username}] 404: usuário não encontrado.")
                return None

            if resp.status_code >= 500:
                logging.warning(f"[{username}] {resp.status_code} servidor indisponível.")
                return None

            try:
                data = resp.json()
            except json.JSONDecodeError:
                logging.error(f"[{username}] JSON inválido.")
                return None

            user = data.get("data", {}).get("user")

            if not user:
                logging.warning(f"[{username}] Campo 'data.user' ausente (bloqueio ou mudança de schema).")
                return None

            followers = (user.get("edge_followed_by") or {}).get("count") 
            following = (user.get("edge_follow") or {}).get("count")
            posts = (user.get("edge_owner_to_timeline_media") or {}).get("count")
            pic_url = user.get("profile_pic_url_hd") or user.get("profile_pic_url")
            bio = user.get("biography")

            return {
                "username": user.get("username"),
                "full_name": user.get("full_name"),
                "bio": bio,
                "posts": posts,
                "followers": followers,
                "following": following,
                "profile_picture_url": pic_url,
                "is_private": user.get("is_private"),
                "is_verified": user.get("is_verified"),
                "scraper_datetime": datetime.now()
            }

        except requests.exceptions.RequestException as e:
            logging.error(f"[{username}] Erro de rede: {e}")
            return None


def process_usernames(
    usernames: List[str],
    out_csv: str = "instagram_profiles1.csv",
    per_user_retries: int = 3,
    sleep_between_attempts: float = 3.0,
) -> List[Dict[str, Any]]:
    """
    Itera sobre uma lista de usernames, tenta múltiplas vezes por usuário,
    acumula resultados e salva em CSV.
    """
    # Opcional: passe o cookie via variável de ambiente para não hardcodar
    sessionid = os.getenv("IG_SESSIONID", "").strip() or None

    scraper = InstagramPublicScraper(
        max_retries=5,
        backoff_factor=1.0,
        per_host_rate=6,
        per_host_window=60,
        timeout=20.0,
        use_requests_cache=True,
        sessionid_cookie=sessionid,
    )

    results: List[Dict[str, Any]] = []
    for username in usernames:
        data = None
        for attempt in range(1, per_user_retries + 1):
            data = scraper.get_profile(username)
            if data:
                data["scraper_datetime"] = datetime.now()
                logging.info(f"[{username}] Sucesso na tentativa {attempt}.")
                break
            logging.info(f"[{username}] Tentativa {attempt} falhou; aguardando {sleep_between_attempts}s…")
            time.sleep(sleep_between_attempts)

        if not data:
            logging.error(f"[{username}] Falhou após {per_user_retries} tentativas.")
            data = {
                "username": username,
                "full_name": None,
                "bio": None,
                "posts": None,
                "followers": None,
                "following": None,
                "profile_picture_url": None,
                "is_private": None,
                "is_verified": None,
                "error": True,
                "scraper_datetime": datetime.now()
            }

        results.append(data)

    # Grava CSV
    fieldnames = ["username", "full_name", "bio", "posts", "followers", "following", "profile_picture_url", "is_private", "is_verified", "scraper_datetime", "error"]
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in results:
            if "error" not in row:
                row["error"] = False
            writer.writerow(row)

    # Resumo no console
    ok = sum(1 for r in results if not r.get("error"))
    fail = len(results) - ok
    logging.info(f"Concluído. OK={ok}, Falhas={fail}. CSV: {out_csv}")
    return results

if __name__ == "__main__":
    USERNAMES = [
        "cruzeiro",
        "atletico",
        "gremio",
        "scinternacional",
        "flamengo",
        "fluminensefc",
        "vascodagama",
        "botafogo",
        "santosfc",
        "palmeiras",
        "corinthians",
        "saopaulofc",
        "redbullbragantino",
        "ecbahia",
        "ecvitoria",
        "ecjuventude",
        "mirassolfc",
        "fortalezaec",
        "cearasc",
        "sportrecife"
    ]
    process_usernames(USERNAMES)
