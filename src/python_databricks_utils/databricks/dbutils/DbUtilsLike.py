from typing import Protocol

from python_databricks_utils.databricks.dbutils.fs.FsLike import FsLike
from python_databricks_utils.databricks.dbutils.notebook.NotebookLike import NotebookLike
from python_databricks_utils.databricks.dbutils.widgets.WidgetsLike import WidgetsLike


class DbUtilsLike(Protocol):
    fs: FsLike
    notebook: NotebookLike
    widgets: WidgetsLike
