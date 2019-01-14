import luigi

class HDF(luigi.Task):
    """
    Wrapper class to encapsulate HDF file requirement
    """

    task_namespace = 'document'

    file_name = luigi.Parameter()

    def requires(self):
        pass

    def output(self):
        return luigi.LocalTarget('{file_name}.hdf'.format(file_name=self.file_name))
    
    def run(self):
        with open(self.file_name) as file: 
            print("{file_name} found".format(file_name=self.file_name))

class TIF(luigi.Task):
    """
    Wrapper class to encapsulate TIF file requirement
    """
    
    task_namespace = 'document'

    file_name = luigi.Parameter()

    def requires(self):
        pass

    def output(self):
        return luigi.LocalTarget('{file_name}.tif'.format(file_name=self.file_name))
    
    def run(self):
        with open(self.file_name) as file: 
            print("{file_name} found".format(file_name=self.file_name))
