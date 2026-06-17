from setuptools import setup, find_packages

setup(
    name="totali",
    version="0.1.0",
    description="TOTaLi-Assisted Defensible Spatial Drafting Pipeline",
    packages=find_packages(),
    python_requires=">=3.10",
    install_requires=[
        "numpy>=1.24.0",
        "scipy>=1.10.0",
        "laspy>=2.5.0",
        "pyproj>=3.5.0",
        "pyyaml>=6.0",
        "click>=8.1.0",
        "pydantic>=2.0.0",
    ],
    extras_require={
        "ml": ["onnxruntime>=1.14.0", "open3d>=0.17.0"],
        "cad": ["ezdxf>=0.18.0"],
        # U6: minimal deps for a green, fully-mocked CI run. Pure-Python only —
        # no native libs (no pdal/open3d/onnxruntime). ezdxf + flask are needed
        # by the CAD-adapter and quarantine-UI tests; the rest of the suite runs
        # against conftest stubs.
        "test": ["pytest>=9.0.2", "ezdxf>=0.18.0", "flask>=3.0.0"],
        "full": [
            "onnxruntime>=1.14.0",
            "open3d>=0.17.0",
            "ezdxf>=0.18.0",
            "pdal>=3.2.0",
            "rich>=13.0.0",
            "matplotlib>=3.7.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "totali-pipeline=totali.main:main",
        ],
    },
)
