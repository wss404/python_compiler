import random
import string


def generate_password(length=12):
    # 定义密码字符集
    characters = string.ascii_letters + string.digits + string.punctuation

    # 生成随机密码
    password = ''.join(random.choice(characters) for _ in range(length))

    return password

# 生成一个长度为 12 的随机密码
random_password = generate_password(12)
print("Random Password:", random_password)
