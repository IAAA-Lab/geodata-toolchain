def run(func):
    print("Before exec")
    def wrapper_run():
        func()
    return wrapper_run