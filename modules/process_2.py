from datetime import datetime as dt
def main():
    with(open("log.txt",'a')) as file:
        file.write(f"Process_2 started  {dt.strftime(dt.now(), '%Y-%m-%d %H-%M-%S')} \n")

