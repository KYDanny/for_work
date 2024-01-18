import requests

def check_section(city, area):
    url = "https://maps.nlsc.gov.tw/T09/pro/setSection.jsp"
    params = {"city": city, "area": area}
    response = requests.get(url, params=params)
    if '0024' in response.text:  # 假設你要檢查的段小段代碼是'0024'
        return True
    return False

def check_landno(city, sect, landno):
    url = "https://api.nlsc.gov.tw/S09_Ralid/getLandInfo"
    params = {"city": city, "sect": sect, "landno": landno}
    response = requests.get(url, params=params)
    if response.json():  # 假設API回傳JSON格式資料
        return True
    return False

def main():
    city = 'V'
    area = 'V16'
    sect = '0024'
    landno = '02870011'

    if check_section(city, area):
        if check_landno(city, sect, landno):
            print("地號有效")
        else:
            print("地號無效")
    else:
        print("段小段失效")

if __name__ == "__main__":
    main()