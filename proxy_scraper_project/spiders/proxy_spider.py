import asyncio
import sys

if sys.platform == "darwin" and sys.version_info >= (3, 8):
    asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())

from twisted.internet import asyncioreactor
asyncioreactor.install()

import base64
import scrapy
import json
import time
from datetime import timedelta
from scrapy.crawler import CrawlerProcess
from scrapy.http import Response, Request
import os

GET_PROXY_URL = f"https://{os.getenv('GET_PROXY_URLS')}/freeproxy"
UPLOAD_PROXY_URL = f"https://{os.getenv('UPLOAD_PROXY_URLS')}/api/post_proxies"
GET_TOKEN_URL = f"https://{os.getenv('UPLOAD_PROXY_URLS')}/api/get_token"
USER_ID = os.getenv("UPLOAD_USER_ID")


class ProxySpider(scrapy.Spider):
    name = "proxy_spider"
    start_urls = [GET_PROXY_URL]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cancelling = False
        self.start_time = time.time()
        self.proxies = []
        self.max_proxies = 150

    def start_requests(self):
        self.logger.info("Starting to scrape proxies...")
        for url in self.start_urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response: Response):
        self.logger.info("Parsing proxy table...")

        rows = response.xpath('//table[@id="table_proxies"]//tr[position()>1]')
        self.logger.info(f"Rows found: {len(rows)}")

        for row in rows:
            if len(self.proxies) >= self.max_proxies:
                break

            tds = row.xpath("./td")
            if len(tds) < 4:
                continue

            ip_encoded = tds[1].attrib.get("data-ip")
            port_encoded = tds[2].attrib.get("data-port")

            if not ip_encoded or not port_encoded:
                continue

            try:
                ip = base64.b64decode(ip_encoded).decode("utf-8")
                port = int(base64.b64decode(port_encoded).decode("utf-8"))
            except Exception:
                continue

            protocol = tds[3].xpath(".//a/text()").get()
            if not protocol:
                continue

            self.proxies.append({"ip": ip, "port": port, "protocols": [protocol]})

        self.logger.info(f"Total proxies collected so far: {len(self.proxies)}")

        if len(self.proxies) >= self.max_proxies:
            self.logger.info("Collected 150 proxies. Saving and uploading...")
            self.proxies = self.proxies[:150]
            with open("proxies.json", "w") as f:
                json.dump(self.proxies, f, indent=2)
            for request in self.upload_proxies():
                yield request
            return

        next_page = response.xpath(
            '//ul[contains(@class, "pagination")]//a[text()="»"]/@href'
        ).get()

        if next_page:
            self.logger.info(f"Following pagination to: {next_page}")
            yield response.follow(next_page, callback=self.parse)
        else:
            self.logger.info("No more pages. Saving and uploading...")
            with open("proxies.json", "w") as f:
                json.dump(self.proxies, f, indent=2)
            for request in self.upload_proxies():
                yield request

    def upload_proxies(self):
        if not USER_ID:
            self.logger.error("UPLOAD_USER_ID is not set.")
            return

        proxy_list = [f"{p['ip']}:{p['port']}" for p in self.proxies[:self.max_proxies]]
        batch_size = 10

        for i in range(0, len(proxy_list), batch_size):
            if self.cancelling:
                self.logger.warning("Upload stopped due to a previous error.")
                break

            batch = proxy_list[i:i + batch_size]
            yield scrapy.Request(
                url=GET_TOKEN_URL,
                callback=self.handle_token_response,
                meta={"batch": batch},
                dont_filter=True
            )
            time.sleep(10)  # <- add 10s delay between requests

    def handle_token_response(self, response: Response):
        batch = response.meta["batch"]
        set_cookie = response.headers.get("Set-Cookie")

        if not set_cookie:
            self.logger.error("No form_token received.")
            return

        try:
            cookie_str = set_cookie.decode().split(";")[0]
            key, value = cookie_str.split("=")
            if key.strip() != "form_token":
                self.logger.error("Invalid form_token format.")
                return
            form_token = value
        except Exception as e:
            self.logger.error(f"Error parsing form_token: {e}")
            return

        payload = {
            "user_id": USER_ID,
            "proxies": ", ".join(batch),
            "len": len(batch),
        }

        self.logger.info(f"Uploading batch with {len(batch)} proxies.")

        yield Request(
            url=UPLOAD_PROXY_URL,
            method="POST",
            body=json.dumps(payload),
            headers={
                "Content-Type": "application/json",
                "User-Agent": "Mozilla/5.0",
                "Cookie": f"form_token={form_token}",
            },
            callback=self.handle_upload,
            errback=self.handle_upload_error,
            meta={"batch": batch.copy()},
            dont_filter=True,
        )

    def handle_upload(self, response: Response):
        batch = response.meta["batch"]
        self.logger.info(f"Received response for batch of {len(batch)} proxies (HTTP {response.status}).")

        try:
            result = json.loads(response.text)
            save_id = result.get("save_id")
            if save_id:
                self.logger.info(f"Saving batch under save_id: {save_id}")
                try:
                    if os.path.exists("results.json"):
                        with open("results.json", "r", encoding="utf-8") as f:
                            data = json.load(f)
                    else:
                        data = {}
                except Exception as e:
                    self.logger.warning(f"Error loading results.json: {e}")
                    data = {}
                data.setdefault(save_id, []).extend(batch)
                with open("results.json", "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2)
                self.logger.info(f"Batch written to results.json under ID {save_id}")
            else:
                self.logger.warning("No save_id found in response.")
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse JSON: {e}")
            self.logger.debug(f"Raw response text: {response.text}")

    def handle_upload_error(self, failure):
        response = getattr(failure.value, "response", None)
        batch = failure.request.meta.get("batch", [])

        if response:
            status = response.status
            self.logger.error(f"Upload failed for batch ({len(batch)} proxies), HTTP {status}")
            if status in (403, 429):
                self.logger.error("Stopping upload due to HTTP error.")
                self.cancelling = True
        else:
            self.logger.error(f"Unknown error occurred during upload: {failure}")
            self.cancelling = True

    def closed(self, reason):
        end_time = time.time()
        total_time = timedelta(seconds=int(end_time - self.start_time))
        with open("time.txt", "w") as f:
            f.write(str(total_time))
        self.logger.info(f"Spider closed. Total run time: {total_time}")


if __name__ == "__main__":
    process = CrawlerProcess(
        settings={
            "LOG_LEVEL": "INFO",
            "USER_AGENT": "Mozilla/5.0",
            "DOWNLOAD_DELAY": 4,
            "AUTOTHROTTLE_ENABLED": True,
            "AUTOTHROTTLE_START_DELAY": 0.5,
            "AUTOTHROTTLE_MAX_DELAY": 2,
            "CONCURRENT_REQUESTS": 4,
            "RETRY_ENABLED": True,
            "RETRY_TIMES": 3,
            "RETRY_HTTP_CODES": [403, 429],
        }
    )
    process.crawl(ProxySpider)
    process.start()
