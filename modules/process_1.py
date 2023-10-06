from datetime import datetime as dt
def main():
    with(open("C:/Users/rodionov.o/Desktop/Work directory./status_control/log.txt",'a')) as file:
        file.write(f"Process_1 started  {dt.strftime(dt.now(), '%Y-%m-%d %H-%M-%S')} \n")

