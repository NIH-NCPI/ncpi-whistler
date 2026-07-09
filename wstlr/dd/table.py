"""

"""

from __future__ import annotations

from typing import Any

from wstlr import die_if, dd_system_url, system_base
from wstlr.dd.variable import DdVariable

_default_subject_id: str = "subject_id"

class DdTable:
    def __init__(self, name: str, study_name: str, description: str = "", **kwargs: Any) -> None:
        self.url_base = kwargs.get("url_base", system_base)
        consent_group = kwargs.get("consent_group")

        self.name = name
        self.description = description

        self.study_name = study_name
        self.consent_group = consent_group
        self.study_component = self.study_name
        if self.consent_group is not None and self.consent_group.strip() != "":
            self.study_component = f"{self.study_name}-{self.consent_group}"

        self.url = dd_system_url(
            self.url_base, "CodeSystem", self.consent_group, self.name, None
        )

        self.variables: dict[str, DdVariable] = {}
        self.key: list[str] = []
        self.subject_id = kwargs.get("subject_id")
        if self.subject_id is None:
            self.subject_id = DdTable.default_subject_id()

    @classmethod
    def default_subject_id(cls, colname: str | None = None) -> str | None:
        global _default_subject_id
        if colname is None:
            return _default_subject_id
        _default_subject_id = colname
        return None

    def add_to_varname_lookup(self, lkup: dict[str, str]) -> None:
        for varname, variable in self.variables.items():
            variable.add_to_varname_lookup(lkup)

    @property
    def vardata(self) -> list[DdVariable]:
        vars = []

        for varname, variable in self.variables.items():
            vars.append(variable)
        return vars

    @property
    def desc(self) -> str:
        if self.description is not None and len(self.description.strip()) > 0:
            return self.description
        return self.name

    @property
    def id_col(self) -> str | None:
        return self.subject_id

    def add_variable(self, **kwargs: Any) -> None:
        var = DdVariable(
            study_name=self.study_name,
            table_name=self.name,
            url_base=self.url_base,
            **kwargs,
        )

        die_if(
            var.varname in self.variables,
            f"{var.varname} appears more than once in definition for "
            f"table, {self.name}",
        )

        self.variables[var.varname] = var
        if var.key_component:
            self.key.append(var.varname)

    def obj_as_dd_variable(self) -> dict[str, Any]:
        """Data dictionary variables do not dump their variable's values"""
        """only variable name/desc"""

        values: list[dict[str, Any]] = []
        for var in self.variables:
            values.append(
                {
                    "code": self.variables[var].varname,
                    "description": self.variables[var].desc,
                }
            )
        obj = {
            "varname": self.name,
            "desc": self.desc,
            "type": "DD-Table",
            "url": self.url,
            "values": values,
        }

        return obj

    def variables_as_cs(self) -> list[dict[str, Any]]:
        variable_cs: list[dict[str, Any]] = []

        for var in self.variables:
            cs = self.variables[var].obj_as_cs()
            if cs is not None:
                variable_cs.append(cs)

        return variable_cs

    def obj_as_dd_table(self) -> dict[str, Any]:
        """Data Dictionary tables list variable's content (but only as code/desc)"""

        variables: list[dict[str, Any]] = []
        for var in self.variables:
            ddvar = self.variables[var].obj_as_dd_variable()
            if ddvar is not None:
                variables.append(ddvar)

        obj = {"table_name": self.name, "url": self.url, "variables": variables}

        return obj

    def obj_as_cs(self) -> dict[str, Any]:
        values: list[dict[str, Any]] = []
        for variable in self.variables:
            values.append(
                {
                    "code": self.variables[variable].varname,
                    "description": self.variables[variable].desc,
                }
            )

        obj = {
            "varname": None,
            "url": self.url,
            "study": self.study_name,
            "values": values,
            "table_name": self.name,
        }

        return obj
