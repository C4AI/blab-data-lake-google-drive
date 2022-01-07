"""Parses a field list as expected by Google Drive API.

    Star is not supported.
"""
from typing import Any

from sly import Lexer, Parser


class FieldLexer(Lexer):
    tokens = {'FIELD', 'COMMA', 'SLASH', 'LPAREN', 'RPAREN'}

    FIELD = r'[a-zA-Z_][a-zA-Z0-9_]*'

    COMMA = r','
    SLASH = r'/'
    LPAREN = r'\('
    RPAREN = r'\)'

    ignore = ' \t\n\r'


ALL_FIELDS = True


# noinspection
class FieldParser(Parser):
    tokens = FieldLexer.tokens

    @_('deepfield')  # type:ignore[name-defined]
    def deepfieldlist(self, p):
        return [p.deepfield]

    @_('deepfieldlist COMMA deepfield')  # type:ignore[name-defined,no-redef]
    def deepfieldlist(self, p):
        return [*p.deepfieldlist, p.deepfield]

    @_('FIELD')  # type:ignore[name-defined]
    def deepfield(self, p):
        return (p.FIELD, ALL_FIELDS)

    @_('FIELD SLASH deepfield')  # type:ignore[name-defined,no-redef]
    def deepfield(self, p):
        return (p.FIELD, [p.deepfield])

    @_('FIELD LPAREN deepfieldlist '  # type:ignore[name-defined,no-redef]
       'RPAREN')
    def deepfield(self, p):
        return (p.FIELD, p.deepfieldlist)

    @_('FIELD LPAREN RPAREN')  # type:ignore[name-defined,no-redef]
    def deepfield(self, p):
        return (p.FIELD, [])


def parse_fields(fields: str) -> Any:
    lexer = FieldLexer()
    parser = FieldParser()
    return parser.parse(lexer.tokenize(fields))


__all__ = ('parse_fields', 'ALL_FIELDS')
