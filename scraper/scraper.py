import requests
import pandas as pd
import json
import base64

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
    )
}

def build_url(page=1, page_size=20, index="IBOV", segment="1", language="pt-br"):
    payload = {
        "language": language,
        "pageNumber": page,
        "pageSize": page_size,
        "index": index,
        "segment": segment
    }
    json_str = json.dumps(payload)
    encoded = base64.urlsafe_b64encode(json_str.encode()).decode()
    return f"https://sistemaswebb3-listados.b3.com.br/indexProxy/indexCall/GetPortfolioDay/{encoded}"

def fetch_b3_data(index="IBOV", segment="1", page_size=20):
    all_results = []
    page = 1

    while True:
        url = build_url(page=page, page_size=page_size, index=index, segment=segment)
        try:
            response = requests.get(url, headers=HEADERS)
            response.raise_for_status()
            content_type = response.headers.get("Content-Type", "")
            if "application/json" not in content_type:
                raise ValueError("Response is not JSON")
            data = response.json()
        except requests.RequestException as e:
            print(f"Request error on page {page}: {e}")
            break
        except ValueError as e:
            print(f"Content error on page {page}: {e}")
            break

        results = data.get("results", [])
        if not results:
            break

        all_results.extend(results)

        total_pages = data.get("page", {}).get("totalPages", 1)
        if page >= total_pages:
            break
        page += 1

    df = pd.DataFrame(all_results)
    return df

if __name__ == "__main__":
    df = fetch_b3_data()
    print(df.head())
