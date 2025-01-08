import geoip2.database

def read_ip_addresses(file_path):
    with open(file_path, 'r') as file:
        return [line.strip() for line in file.readlines()]

def get_country_info(ip_address, reader):
    try:
        response = reader.country(ip_address)
        return response.country.names.get('zh-CN', response.country.name)
    except geoip2.errors.AddressNotFoundError:
        return "Unknown"

def write_country_info(ip_addresses, output_file, reader):
    with open(output_file, 'w') as file:
        for ip in ip_addresses:
            country = get_country_info(ip, reader)
            file.write(f"{ip}: {country}\n")

def main():
    # 读取 IP 地址
    # ip_addresses_1 = read_ip_addresses('1.txt')
    # ip_addresses_2 = read_ip_addresses('2.txt')
    ip_addresses = read_ip_addresses("1.txt")

    # 打开 GeoLite2 数据库
    reader = geoip2.database.Reader('GeoLite2-Country.mmdb')

    # 写入国家信息到 out.txt
    write_country_info(ip_addresses, 'out.txt', reader)

    # 关闭数据库
    reader.close()

if __name__ == "__main__":
    main()