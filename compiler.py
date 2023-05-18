#! /usr/bin/python
"""
python项目编译脚本
usage: ./compiler path/to/project n
        其中n是编译进程数量，默认为机器的核心数
example: ./compiler.py ./data-exchange-platform/ 256
"""
import os
import signal
import sys
from multiprocessing import cpu_count, Pool

ignored = ["migrations", '__init__.py', 'manage.py']


def dir_travel(path):
    """遍历目录"""
    for root, dirs, files in os.walk(path):
        for file in files:
            yield root, file


def batch_compiler(path, process):
    """整体处理逻辑"""
    backup_dir(path)  # 备份项目目录
    install_cython()
    process_pool = Pool(processes=process)
    for root, file in dir_travel(path):
        process_pool.apply_async(compile_path, (root, file), error_callback=throw_error)
    process_pool.close()
    process_pool.join()
    rm_build()


def throw_error(e):
    print("~~~~~~~~~~~~~~~~COMPILER ERROR~~~~~~~~~~~~~~~~~~", e.__cause__)
    os.killpg(os.getpgid(os.getpid()), signal.SIGKILL)


def compile_path(root, file):
    try:
        if root.endswith("migrations") or file in ignored:
            return

        if file.endswith(".py"):
            # 项目内部目录路径 组成字符串 用于标识不同路径下的同名文件
            unique_path = "_".join(["_".join(root.strip("./").split("/")[1:]), file[:-3]])
            file_path = os.path.join(root, file)  # 文件全路径或相对当前目录的路径
            print("current filename: ", file_path)
            adjust_script_content(file_path, unique_path)  # 修改setup.py最后一行中的文件名
            compile_file(unique_path)  # 编译文件
            rm_file(file_path)  # 删除py文件
            move_so_to(file, root, unique_path)  # 将so文件从build目录移到py文件目录
    except Exception as e:
        print(e)
        raise e


def compile_file(unique_path):
    """编译为so文件"""
    cmd = f"""python {unique_path}_setup.py build_ext"""
    os.system(cmd)


def move_so_to(file, root, unique_path):
    """file是原文件名，root是目标路径，unique_path用于区分不同路径下的同名文件"""
    if not os.path.isdir(root):
        raise Exception(f"{root}不是目录")
    file = file[:-3]  # 删除.py
    source = search_file(file, unique_path)
    # source = f"./build/lib.linux-x86_64-3.6/{new_path}/{file}.*.so"
    destination = f"{root}/{file}.so"
    cmd = f"""mv {source} {destination}"""
    os.system(cmd)
    c_file = os.path.join(root, file + ".c")
    cmd = f"""rm {c_file}"""
    os.system(cmd)
    cmd = f"""rm {unique_path}_setup.py"""
    os.system(cmd)


def search_file(target, unique_path):
    root = "./build"
    # 第一遍按匹配目标路径
    for dir_name, file in dir_travel(root):
        u_path = "_".join([dir_name.strip("./").replace("/", "_"), file])
        if file.endswith(".so") and file.startswith(target) and unique_path in u_path:
            return os.path.join(dir_name, file)
    # 第二遍全局搜索
    for dir_name, file in dir_travel(root):
        if file.endswith(".so") and file.startswith(target):
            return os.path.join(dir_name, file)

    raise Exception(f"未找到文件{target}, unique_path {unique_path}")


def file_exists_in_path(file, path):
    """查找当前目录及其父级目录中是否存在特定文件"""
    path = path.strip("./")
    if os.path.exists("/".join([path, file])):
        return True
    return False


def adjust_script_content(file_path, unique_path):
    """两处文件需要修改"""
    adjust_setup(file_path, unique_path)
    adjust_file_header(file_path)


def adjust_file_header(file_path):
    """在文件头部加上一行指定解释器版本"""
    cmd = f"""sed -i '1i\# cython: language_level=3' {file_path}"""
    os.system(cmd)


def adjust_setup(file_path, unique_path):
    """将第三行中的文件名替换为当前文件名"""
    cmd = f"""echo "from distutils.core import setup
from Cython.Build import cythonize
setup(ext_modules=cythonize(['{file_path}']))
" > {unique_path}_setup.py"""
    os.system(cmd)


def rm_file(file_path):
    """删除py文件"""
    os.remove(file_path)


def backup_dir(path):
    cmd = f"""rm /tmp/{path}"""
    os.system(cmd)
    cmd = f"""cp -r {path} /tmp/{path}"""
    os.system(cmd)


def restore_dir(path):
    cmd = f"""rm -rf {path}"""
    os.system(cmd)
    cmd = f"""mv {path}_backup {path}"""
    os.system(cmd)


def rm_build():
    cmd = f"""rm -rf build"""
    os.system(cmd)
    cmd = f"""rm -f setup.py"""
    os.system(cmd)


def execute_command(cmd):
    os.popen(cmd)


def install_cython():
    cmd = "pip install Cython -i https://pypi.doubanio.com/simple/"
    os.system(cmd)


if __name__ == '__main__':
    path_ = sys.argv[1]
    n = cpu_count()
    if len(sys.argv) > 2:
        n = int(sys.argv[2])
    batch_compiler(path_, n)
