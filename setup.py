from setuptools import setup

setup(
    name="Vnode",
    version="0.1.0",
    description="A fast wheel to build a node network.",
    author="Tatersic&Ovizro",
    author_email="Tatersic@qq.com",
    maintainer="Ovizro",
    maintainer_email="Ovizro@hypercol.com",

    license="MIT",
    url="https://github.com/Tatersic/Vnode",
    download_url="https://github.com/Tatersic/Vnode/archive/refs/heads/master.zip",

    packages=["vnode"],
    requires=["aiohttp"],
    python_requires=">=3.8",

    exclude_package_data={
        '':['test.*']
    }
)