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
        self.form_token = None

    def start_requests(self):
        self.logger.info("Requesting form_token...")
        yield scrapy.Request(
            url=GET_TOKEN_URL,
            callback=self.get_token,
            dont_filter=True
        )

    def get_token(self, response: Response):
        set_cookie = response.headers.get("Set-Cookie")
        if not set_cookie:
            self.logger.error("form_token not found in Set-Cookie header.")
            return

        try:
            cookie_str = set_cookie.decode().split(";")[0]
            key, value = cookie_str.split("=")
            if key.strip() != "form_token":
                self.logger.error("Invalid form_token format.")
                return
            self.form_token = value
        except Exception as e:
            self.logger.error(f"Error parsing form_token: {e}")
            return

        self.logger.info(f"form_token acquired: {self.form_token}")
        for url in self.start_urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response: Response):
        self.logger.info("Parsing proxy table...")

        rows = response.xpath('//table[@id="table_proxies"]//tr[position()>1]')
        self.logger.info(f"Rows found: {len(rows)}")

        for i, row in enumerate(rows):
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

        next_page = response.xpath(
            '//ul[contains(@class, "pagination")]//a[text()="»"]/@href'
        ).get()

        if next_page:
            self.logger.info(f"Following pagination to next page: {next_page}")
            yield response.follow(next_page, callback=self.parse)
        else:
            self.logger.info("Last page reached. Saving and uploading proxies.")

            with open("proxies.json", "w") as f:
                json.dump(self.proxies, f, indent=2)

            yield from self.upload_proxies()

    def upload_proxies(self):
        if not USER_ID:
            self.logger.error("UPLOAD_USER_ID is not set.")
            return

        self.cancelling = False
        proxy_list = [f"{p['ip']}:{p['port']}" for p in self.proxies]
        batch_size = 10

        for i in range(0, len(proxy_list), batch_size):
            if self.cancelling:
                self.logger.warning("Upload stopped due to previous error (403/429).")
                break

            batch = proxy_list[i:i + batch_size]
            payload = {
                "user_id": USER_ID,
                "proxies": ", ".join(batch),
                "len": len(batch),
            }

            self.logger.info(f"Uploading batch {i // batch_size + 1} with {len(batch)} proxies.")
            self.logger.debug(json.dumps(payload, indent=2))

            yield Request(
                url=UPLOAD_PROXY_URL,
                method="POST",
                body=json.dumps(payload),
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": "Mozilla/5.0",
                    "Cookie": f"form_token={self.form_token}",
                },
                callback=self.handle_upload,
                errback=self.handle_upload_error,
                meta={"batch": batch},
            )

    def handle_upload(self, response: Response):
        batch = response.meta["batch"]
        self.logger.info(f"Received response for batch of {len(batch)} proxies (HTTP {response.status}).")

        try:
            result = json.loads(response.text)
            if "detail" in result:
                self.logger.warning(f"Server warning: {result['detail']}")
            with open("results.json", "a") as f:
                f.write(json.dumps(result, indent=2) + "\n")
        except json.JSONDecodeError:
            self.logger.warning("Response is not valid JSON.")

    def handle_upload_error(self, failure):
        response = getattr(failure.value, "response", None)
        batch = failure.request.meta.get("batch", [])

        if response:
            status = response.status
            self.logger.error(f"Upload failed for batch ({len(batch)} proxies) with HTTP {status}.")

            if status in (403, 429):
                self.logger.error("Upload stopped due to HTTP status: %d", status)
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
    process = CrawlerProcess(settings={
        "LOG_LEVEL": "INFO",
        "USER_AGENT": "Mozilla/5.0",
    })
    process.crawl(ProxySpider)
    process.start()
