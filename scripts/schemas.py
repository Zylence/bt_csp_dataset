from typing import Mapping, Any
import pyarrow as pa
import json
from jsonschema.validators import validate


class Constants:
    PROBLEM_ID = "problemId"
    MODEL_NAME = "modelName"
    FEATURE_VECTOR = "feature_vector"

    FLAT_BOOL_VARS = "flatBoolVars"
    FLAT_INT_VARS = "flatIntVars"
    FLAT_SET_VARS = "flatSetVars"
    ID_TO_VAR_NAME_MAP = "idToVarNameMap"
    ID_TO_CONSTRAINT_NAME_MAP = "idToConstraintNameMap"
    DOMAIN_WIDTHS = "domainWidths"
    STD_DEVIATION_DOMAIN = "stdDeviationDomain"
    AVERAGE_DOMAIN_SIZE = "averageDomainSize"
    MEDIAN_DOMAIN_SIZE = "medianDomainSize"
    AVERAGE_DOMAIN_OVERLAP = "averageDomainOverlap"
    NUMBER_OF_DISJOINT_PAIRS = "numberOfDisjointPairs"
    META_CONSTRAINTS = "metaConstraints"
    TOTAL_CONSTRAINTS = "totalConstraints"
    AVG_DECISION_VARS_IN_CONSTRAINTS = "avgDecisionVarsInConstraints"
    CONSTRAINT_GRAPH = "constraintGraph"
    CONSTRAINT_HISTOGRAM = "constraintHistogram"
    ANNOTATION_HISTOGRAM = "annotationHistogram"
    METHOD = "method"
    FLAT_ZINC = "flatZinc"

    ####
    INSTANCE_RESULTS = "instanceResults"

    OUTPUT_TYPE = "type"
    SOLVER_STATISTICS = "statistics"
    INSTANCE_PERMUTATION = "instancePermutation"
    ID = "id"  #unique

    INIT_TIME = "initTime"
    SOLVE_TIME = "solveTime"
    SOLUTIONS = "solutions"
    VARIABLES = "variables"
    PROPAGATORS = "propagators"
    PROPAGATIONS = "propagations"
    NODES = "nodes"
    FAILURES = "failures"   #backtracks
    RESTARTS = "restarts"
    PEAK_DEPTH = "peakDepth"


class Schemas:
    class Parquet:
        feature_vector: pa.Schema = pa.schema([
            pa.field(Constants.PROBLEM_ID, pa.string(), False),
            pa.field(Constants.MODEL_NAME, pa.string(), False),     # this property does not exist in json feature vector
            pa.field(Constants.FLAT_BOOL_VARS, pa.int32(), False),
            pa.field(Constants.FLAT_INT_VARS, pa.int32(), False),
            pa.field(Constants.FLAT_SET_VARS, pa.int32(), False),
            pa.field(Constants.ID_TO_VAR_NAME_MAP, pa.map_(pa.int32(), pa.string(), True), False),  # may not work
            pa.field(Constants.ID_TO_CONSTRAINT_NAME_MAP, pa.map_(pa.int32(), pa.string(), True), False),
            pa.field(Constants.DOMAIN_WIDTHS, pa.list_(pa.int64()), False),
            pa.field(Constants.STD_DEVIATION_DOMAIN, pa.float64(), False),
            pa.field(Constants.AVERAGE_DOMAIN_SIZE, pa.float64(), False),
            pa.field(Constants.MEDIAN_DOMAIN_SIZE, pa.int64(), False),
            pa.field(Constants.AVERAGE_DOMAIN_OVERLAP, pa.float64(), False),
            pa.field(Constants.NUMBER_OF_DISJOINT_PAIRS, pa.int32(), False),
            pa.field(Constants.META_CONSTRAINTS, pa.int32(), False),
            pa.field(Constants.TOTAL_CONSTRAINTS, pa.int32(), False),
            pa.field(Constants.AVG_DECISION_VARS_IN_CONSTRAINTS, pa.float64(), False),
            pa.field(Constants.CONSTRAINT_GRAPH, pa.string(), False),
            pa.field(Constants.CONSTRAINT_HISTOGRAM, pa.map_(pa.string(), pa.int32(), True), False),
            pa.field(Constants.ANNOTATION_HISTOGRAM, pa.map_(pa.string(), pa.int32(), True), False),
            pa.field(Constants.METHOD, pa.string(), False),
            pa.field(Constants.FLAT_ZINC, pa.string(), False),
        ])

        __irs = [
            pa.field(Constants.MODEL_NAME, pa.string(), nullable=False),
            pa.field(Constants.ID, pa.int64(), nullable=False),
            pa.field(Constants.INSTANCE_PERMUTATION, pa.list_(pa.string()), nullable=False),
            pa.field(Constants.INIT_TIME, pa.float64(), nullable=False),
            pa.field(Constants.SOLVE_TIME, pa.float64(), nullable=False),
            pa.field(Constants.SOLUTIONS, pa.int64(), nullable=False),
            pa.field(Constants.VARIABLES, pa.int64(), nullable=False),
            pa.field(Constants.PROPAGATORS, pa.int64(), nullable=False),
            pa.field(Constants.PROPAGATIONS, pa.int64(), nullable=False),
            pa.field(Constants.NODES, pa.int64(), nullable=False),
            pa.field(Constants.FAILURES, pa.int64(), nullable=False),
            pa.field(Constants.RESTARTS, pa.int64(), nullable=False),
            pa.field(Constants.PEAK_DEPTH, pa.int64(), nullable=False),
        ]

        # struct
        instance_result: pa.DataType = pa.struct(__irs)

        # schema
        instance_results: pa.Schema = pa.schema(
            __irs
        )

        instances: pa.Schema = pa.schema(
            [
                pa.field(Constants.MODEL_NAME, pa.string(), False),
                pa.field(Constants.ID, pa.int64(), False),
                pa.field(Constants.INSTANCE_PERMUTATION, pa.list_(pa.string()), False),
            ]
        )

    class JSON:
        feature_vector: Mapping[str, Any] = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                Constants.OUTPUT_TYPE: {
                    "type": "string",
                    "enum": [Constants.FEATURE_VECTOR]
                },
                Constants.FEATURE_VECTOR: {
                    "type": "object",
                    "properties": {
                        Constants.FLAT_BOOL_VARS: {
                            "type": "integer"
                        },
                        Constants.FLAT_INT_VARS: {
                            "type": "integer"
                        },
                        Constants.FLAT_SET_VARS: {
                            "type": "integer"
                        },
                        Constants.ID_TO_VAR_NAME_MAP: {
                            "type": "object",
                            "additionalProperties": {
                                "type": "string"
                            }
                        },
                        Constants.ID_TO_CONSTRAINT_NAME_MAP: {
                            "type": "object",
                            "additionalProperties": {
                                "type": "string"
                            }
                        },
                        Constants.DOMAIN_WIDTHS: {
                            "type": "array",
                            "items": {
                                "type": "integer"
                            }
                        },
                        Constants.STD_DEVIATION_DOMAIN: {
                            "type": "number"
                        },
                        Constants.AVERAGE_DOMAIN_SIZE: {
                            "type": "number"
                        },
                        Constants.MEDIAN_DOMAIN_SIZE: {
                            "type": "integer"
                        },
                        Constants.AVERAGE_DOMAIN_OVERLAP: {
                            "type": "number"
                        },
                        Constants.NUMBER_OF_DISJOINT_PAIRS: {
                            "type": "integer"
                        },
                        Constants.META_CONSTRAINTS: {
                            "type": "integer"
                        },
                        Constants.TOTAL_CONSTRAINTS: {
                            "type": "integer"
                        },
                        Constants.AVG_DECISION_VARS_IN_CONSTRAINTS: {
                            "type": "number"
                        },
                        Constants.CONSTRAINT_GRAPH: {
                            "type": "string"
                        },
                        Constants.CONSTRAINT_HISTOGRAM: {
                            "type": "object",
                            "additionalProperties": {
                                "type": "integer"
                            }
                        },
                        Constants.ANNOTATION_HISTOGRAM: {
                            "type": "object",
                            "additionalProperties": {
                                "type": "integer"
                            }
                        },
                        Constants.METHOD: {
                            "type": "string"
                        }
                    },
                    "required": [
                        Constants.FLAT_BOOL_VARS,
                        Constants.FLAT_INT_VARS,
                        Constants.FLAT_SET_VARS,
                        Constants.ID_TO_VAR_NAME_MAP,
                        Constants.ID_TO_CONSTRAINT_NAME_MAP,
                        Constants.DOMAIN_WIDTHS,
                        Constants.STD_DEVIATION_DOMAIN,
                        Constants.AVERAGE_DOMAIN_SIZE,
                        Constants.MEDIAN_DOMAIN_SIZE,
                        Constants.AVERAGE_DOMAIN_OVERLAP,
                        Constants.NUMBER_OF_DISJOINT_PAIRS,
                        Constants.META_CONSTRAINTS,
                        Constants.TOTAL_CONSTRAINTS,
                        Constants.AVG_DECISION_VARS_IN_CONSTRAINTS,
                        Constants.CONSTRAINT_GRAPH,
                        Constants.CONSTRAINT_HISTOGRAM,
                        Constants.ANNOTATION_HISTOGRAM,
                        Constants.METHOD
                    ]
                }
            },
            "required": [
                Constants.OUTPUT_TYPE,
                Constants.FEATURE_VECTOR
            ]
        }

        solver_statistics = {
            "type": "object",
            "properties": {
                Constants.OUTPUT_TYPE: {
                    "type": "string",
                    "enum": [Constants.SOLVER_STATISTICS]
                },
                "statistics": {
                    "type": "object",
                    "properties": {
                        Constants.INIT_TIME: {"type": "number"},
                        Constants.SOLVE_TIME: {"type": "number"},
                        Constants.SOLUTIONS: {"type": "integer"},
                        Constants.VARIABLES: {"type": "integer"},
                        Constants.PROPAGATORS: {"type": "integer"},
                        Constants.PROPAGATIONS: {"type": "integer"},
                        Constants.NODES: {"type": "integer"},
                        Constants.FAILURES: {"type": "integer"},
                        Constants.RESTARTS: {"type": "integer"},
                        Constants.PEAK_DEPTH: {"type": "integer"}
                    },
                    "required": [
                                Constants.INIT_TIME,
                                Constants.SOLVE_TIME,
                                Constants.SOLUTIONS,
                                Constants.VARIABLES,
                                Constants.PROPAGATORS,
                                Constants.PROPAGATIONS,
                                Constants.NODES,
                                Constants.FAILURES,
                                Constants.RESTARTS,
                                Constants.PEAK_DEPTH,
                                ],
                    "additionalProperties": False
                }
            },
            "required": [Constants.OUTPUT_TYPE, Constants.SOLVER_STATISTICS],
            "additionalProperties": False
        }


class Helpers:

    @staticmethod
    def parse_json_validated(maybe_json: str, schema: Mapping[str, Any]):
        """Throws if validating fails"""
        data = json.loads(maybe_json)
        validate(instance=data, schema=schema)
        return data

    @staticmethod
    def normalize_dict(data: dict, keys: list[str], conversion: Any):
        for key in keys:
            if key in data:
                data[key] = conversion(data[key])
            else:
                raise ValueError(f"Key {key} not in dictionary {data}, conversion cannot be applied")

    # JSON does not allow int keys, but parquet does.
    @staticmethod
    def __type_conversion_map_keys_str_to_int(map):
        return {int(k): str(v) for k, v in map.items()}

    @staticmethod
    def json_to_normalized_feature_vector_dict(maybe_json: str) -> dict:
        """
        Reads a JSON string that represents a feature vector and normalizes it.

        Parameters:
            maybe_json (str): The JSON string representing the feature vector.

        Returns:
            dict: The normalized feature vector as a dictionary.
        """
        js = Helpers.parse_json_validated(maybe_json, Schemas.JSON.feature_vector)
        feature_vector = js[Constants.FEATURE_VECTOR]
        Helpers.normalize_dict(
            feature_vector,
            [Constants.ID_TO_VAR_NAME_MAP, Constants.ID_TO_CONSTRAINT_NAME_MAP],
            Helpers.__type_conversion_map_keys_str_to_int
        )
        return feature_vector

    @staticmethod
    def json_to_solution_statistics_dict(maybe_json: str) -> dict:
        """
        Reads a JSON string that represents solver statistics and converts it to a dictionary.

        Parameters:
            maybe_json (str): The JSON string representing the solver statistics.

        Returns:
            dict: The solver statistics as a dictionary.
        """
        js = Helpers.parse_json_validated(maybe_json, Schemas.JSON.solver_statistics)
        return js[Constants.SOLVER_STATISTICS]