from concurrent.futures.thread import ThreadPoolExecutor
import threading
import warnings
import time
import sys
import botocore.exceptions

_EXECUTOR = ThreadPoolExecutor()


# 单个并发任务类, 实现python异步非阻塞函数
class ConcurrentTask:
    def __init__(self,
                 executor: ThreadPoolExecutor = None,
                 task=None,
                 t_args: tuple = None,
                 t_kwargs: dict = None,
                 callback=None,
                 c_args: tuple = None,
                 c_kwargs: dict = None):
        """
        init
        :param executor: python.concurrent.futures ThreadingPoolExecutor实例
        :param task: 并发任务函数
        :param t_args: 并发任务函数输入参数
        :param t_kwargs: 并发任务函数输入关键字参数
        :param callback: 回调函数
        :param c_args: 回调函数输入参数
        :param c_kwargs: 回调函数输入关键字参数
        """
        self._task = task
        self._callbackTask = callback
        self._t_args = t_args if t_args else ()
        self._c_args = c_args if c_args else ()
        self._t_kwargs = t_kwargs if t_kwargs else dict()
        self._c_kwargs = c_kwargs if c_kwargs else dict()
        self._future = None
        self._executor = executor if executor else _EXECUTOR

    def run(self):
        """
        开启新线程运行并发任务并在有结果返回时运行回调函数
        :return:
        """
        self._future = self._executor.submit(self._task, *self._t_args, **self._t_kwargs)
        if self._callbackTask is not None:
            self._future.add_done_callback(self.__callback)
        return self

    def __callback(self, future):
        """
        传入并发任务的返回结果并执行回调函数
        :param future: python.concurrent.futures future实例
        :return:
        """
        if self._callbackTask is None:
            raise NotImplementedError
        try:
            self._callbackTask(*self._c_args, **self._c_kwargs, result=future.result())
        except Exception as e:
            print(e)
            self._callbackTask(*self._c_args, **self._c_kwargs)

    def get_result(self):
        """
        获取单个并发任务执行结果
        :return: 执行结果
        """
        return self._future.result()

    @property
    def task(self):
        return self._task

    @property
    def t_args(self):
        return self._t_args

    @property
    def t_kwargs(self):
        return self._t_kwargs


# 多并发任务管理池
class ConcurrentTaskPool:
    def __init__(self, executor: ThreadPoolExecutor = None):
        """
        init
        :param executor: python.concurrent.futures ThreadingPoolExecutor实例
        """
        self._tasks = []
        self._executor = executor if executor else _EXECUTOR

    def add(self, tasks: [ConcurrentTask, [ConcurrentTask]]):
        """
        单个/批量注册并执行并发任务
        :param tasks: 并发任务实例 / 并发任务列表 [ConcurrentTask, ConcurrentTask, ...]
        :return:
        """
        if isinstance(tasks, ConcurrentTask):
            tasks = [tasks]

        for t in tasks:
            t.future = self._executor.submit(t.task, *t.t_args, **t.t_kwargs)
            self._tasks.append(t)
        return self

    def get_results(self, merge: bool = False, ignore_null: bool = False, wait: bool = False):
        """
        获取并发任务执行结果 (当最晚执行完毕的任务结束后返回结果, 此间线程阻塞)
        :param merge: 是否将各任务结果合并为一个列表返回
        :param ignore_null: 是否在收集返回结果时跳过 None 值
        :param wait: 设定获取结果时线程是否阻塞
        :return: 返回结果列表
        """
        self._executor.shutdown(wait=wait)
        results = []
        for task in self._tasks:
            result = task.future.result()
            if ignore_null and result is None:
                continue
            results.extend(result) if merge else results.append(result)
        # 获取所有并发任务结果后清空任务列表
        self._tasks.clear()
        return results


class Timer(threading.Thread):
    def __init__(self, interval: [int, float] = .05, daemon=False):
        """
        继承于 threading.Thread, 每创建并运行一个 Timer 实例即进入一个新的线程,
        在其中运行 while 计时循环, 并以一定时间间隔循环更新 时, 分, 秒 等时间状态 (__min, __sec 等)
        可通过 terminate() 关闭定时器并退出线程,
        可通过 for_each, for_min, for_sec 等装饰器重写不同定时任务的方法, 其分别对应 每次循环, 每分, 每秒 需执行的定时任务

        :param interval: 定时器每次循环时间间隔 (默认值为 0.05 秒)
        :param daemon: Thread.daemon (为 False 时需调用 terminate() 手动退出)

        Example:
            timer = Timer()

            @timer.for_sec
            def fn(sec):
                print(f'{sec}s...')

            timer.start()
        """

        super().__init__(daemon=daemon)
        self.__daemon = daemon
        self.__interval = interval
        self.__period = 1
        self.__freq = int(1 / interval)
        self.__idx = 0
        self.__sec = 0
        self.__min = 0
        self.__tasks = dict()
        self.__for_each = list()
        self.__for_sec = list()
        self.__for_min = list()
        self.__exit = False

    def __counter(self, idx):
        if idx % self.__freq == 0:
            self.__sec += 1
            for for_sec in self.__for_sec:
                for_sec()

            if self.__sec % 60 == 0 and self.__sec != 0:
                self.__min += 1
                for for_min in self.__for_min:
                    for_min()

    def run(self):
        print('timer starts')
        while not self.__exit:
            self.__idx += 1

            self.__counter(self.__idx)

            for for_each in self.__for_each:
                for_each()

            time.sleep(self.__interval)

            if self.__idx == sys.maxsize - 7:
                self.__reset()

        print('timer ends')

    def for_each(self, func):
        def wrp(*args, **kwargs):
            func(*args, **kwargs, idx=self.__idx)
        self.__for_each.append(wrp)

    def for_sec(self, func):
        def wrp(*args, **kwargs):
            func(*args, **kwargs, sec=self.__sec)
        self.__for_sec.append(wrp)

    def for_min(self, func):
        def wrp(*args, **kwargs):
            func(*args, **kwargs, min=self.__min)
        self.__for_min.append(wrp)

    def get_idx(self):
        return self.__idx

    def get_sec(self):
        return self.__sec

    def get_min(self):
        return self.__min

    def terminate(self):
        self.__exit = True

    def __reset(self):
        warnings.warn('timer' + str(id(self)) + ' has been reset', stacklevel=2)
        self.__idx = 0
        self.__sec = 0
        self.__min = 0
        self.__period += 1
