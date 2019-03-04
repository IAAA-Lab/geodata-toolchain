from multipledispatch import dispatch


class Stage:
    def __init__(self):
        self.task_list = []
        self.resource_output_list = [{'task': None, 'out_res': None}]

    @dispatch(list)
    def add(self, task_list):
        self.task_list += task_list

        return self

    @dispatch(object)
    def add(self, task):
        self.task_list += [task]

        return self

    def run(self):
        for task in self.task_list:
            self.resource_output_list += \
                [{'task': task.__str__, 'out_res': task.run()}]  # self.resource_output_list[-1]['out_res']

        return self
