class LakeflowDbtError(Exception):
    """Base error for lakeflow-dbt."""


class ConfigError(LakeflowDbtError):
    """Invalid project or model configuration."""


class ParseError(LakeflowDbtError):
    """A model file or schema.yml could not be parsed."""


class GraphError(LakeflowDbtError):
    """The model graph is invalid (cycles, unknown refs, ...)."""


class CompilationError(LakeflowDbtError):
    """A model could not be compiled to declarative pipeline SQL."""
