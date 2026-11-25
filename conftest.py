
# conftest.py
# Run Delta integration tests before other Spark tests.

def pytest_collection_modifyitems(items):
    items.sort(
        key=lambda item: (
            0 if "test_delta_merge.py" in str(item.path)
            else 1
        )
    )
