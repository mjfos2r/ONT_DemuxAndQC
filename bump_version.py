import sys
import tomllib
import tomli_w
from pathlib import Path


def bump(version, level):
    major, minor, patch = map(int, version.split("."))
    match level:
        case "patch":
            patch += 1
        case "minor":
            minor += 1
            patch = 0
        case "major":
            major += 1
            minor = patch = 0
        case "init":
            major = minor = patch = 0
    return f"{major}.{minor}.{patch}"


def get_version(file) -> str:
    with open(file, "r") as f:
        return f.readline().strip()


def bump_version(file, version):
    with open(file, "w") as f:
        f.write(version)


def bump_toml(file, version):
    with open(file, "r+b") as f:
        config = tomllib.load(f)
        f.seek(0)
        print("Current version: %s" % config["project"]["version"])
        config["project"]["version"] = version
        tomli_w.dump(config, f)


def main():
    main_version_file = Path(".VERSION")
    pyproject_toml = Path("pyproject.toml")

    if len(sys.argv) != 2:
        print("Usage: %s [-p|-m|-M|<version>]" % sys.argv[0])
        sys.exit(1)

    old_version = get_version(main_version_file)
    arg = sys.argv[1]
    match arg:
        case "-p":
            new_version = bump(old_version, "patch")
        case "-m":
            new_version = bump(old_version, "minor")
        case "-M":
            new_version = bump(old_version, "major")
        case "--init":
            new_version = bump("0.0.0", "init")
        case _:
            new_version = arg

    if not main_version_file.exists() and pyproject_toml.exists():
        print(
            "ERROR: Missing version files! pass --init to create a blank VERSION file."
        )
        sys.exit(1)

    print(f"Old: {old_version}")
    print(f"New: {new_version}")
    try:
        print("Bumping version in pyproject.toml")
        bump_toml(pyproject_toml, new_version)
    except FileNotFoundError:
        sys.exit(1)
    finally:
        print(f"Bumping version in {main_version_file}")
        bump_version(main_version_file, new_version)
        sys.exit(0)


if __name__ == "__main__":
    main()
