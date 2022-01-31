class LabelClass:
    def __init__(self, name, metadata, unique_id=None):
        self.name = name
        self.metadata = metadata
        self.class_id = unique_id

    def to_dic(self):
        dic = {
            "name": self.name,
            "metadata": self.metadata}
        if self.class_id:
            dic.update({"unique-id": self.class_id})
        return dic
