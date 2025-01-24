import requests
import xml.etree.ElementTree as ET


try:
    req_url = f"https://www.seogwipo.go.kr/openapi/goodRestaurantService/"
    res: requests.Response = requests.get(
        req_url,
        timeout=30,
    )
    res.raise_for_status()  # HTTP 에러 발생 시 예외 발생
    root = ET.fromstring(res.content)
    
    res_data = []
    items = root.findall(".//item")
    for item in items:
        record = {
            "year": item.find("year").text,
            "resto_nm": item.find("resto_nm").text,
            "address": item.find("address").text,
            "lati": item.find("lati").text,
            "longi": item.find("longi").text,
            "tel": item.find("tel").text.strip(),
            "kind": item.find("kind").text,
            "mfood": item.find("mfood").text,
            "dong": item.find("dong").text,
            "appoint": item.find("appoint").text,
            "ldate": item.find("ldate").text,
        }
        res_data.append(record)
    

    # print(total[0].text)
    # result = {child.tag: child.text for child in total} 
    print(res_data)

    # res_data = json.loads(res.content)["data"]
    # logging.info(f"Data retrieved: {len(res_data)} records")
    # context["task_instance"].xcom_push(key="res_data", value=res_data)
except requests.exceptions.RequestException as e:
    # logging.error(f"API request failed: {e}")
    raise e
