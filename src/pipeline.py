from src.load_raw_data import main as load_raw
from src.transform_posts import main as transform
from src.create_user_stats import main as mart


def main():
    load_raw()
    transform()
    mart()


if __name__ == "__main__":
    main()