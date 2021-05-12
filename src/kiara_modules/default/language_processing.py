# -*- coding: utf-8 -*-
import nltk
import pyarrow as pa
import typing
from pydantic import Field

from kiara import KiaraModule
from kiara.config import KiaraModuleConfig
from kiara.data.values import ValueSchema
from kiara.exceptions import KiaraProcessingException
from kiara.module import StepInputs, StepOutputs


def get_stopwords():

    # TODO: make that smarter
    pass

    # nltk.download('punkt')
    # nltk.download('stopwords')
    from nltk.corpus import stopwords

    return stopwords


class TokenizeTextConfig(KiaraModuleConfig):

    filter_non_alpha: bool = Field(
        description="Whether to filter out non alpha tokens.", default=True
    )
    min_token_length: int = Field(description="The minimum token length.", default=3)
    to_lowercase: bool = Field(
        description="Whether to lowercase the tokens.", default=True
    )


class TokenizeTextModule(KiaraModule):

    _config_cls = TokenizeTextConfig

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        inputs = {"text": {"type": "string", "doc": "The text to tokenize."}}

        return inputs

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        outputs = {
            "token_list": {
                "type": "list",
                "doc": "The tokenized version of the input text.",
            }
        }
        return outputs

    def process(self, inputs: StepInputs, outputs: StepOutputs) -> None:

        # TODO: module-independent caching?

        # language = inputs.get_value_data("language")
        #
        text = inputs.get_value_data("text")
        tokenized = nltk.word_tokenize(text)

        result = tokenized
        if self.get_config_value("min_token_length") > 0:
            result = (
                x
                for x in tokenized
                if len(x) >= self.get_config_value("min_token_length")
            )

        if self.get_config_value("filter_non_alpha"):
            result = (x for x in result if x.isalpha())

        if self.get_config_value("to_lowercase"):
            result = (x.lower() for x in result)

        outputs.token_list = list(result)


class RemoveStopwordsModule(KiaraModule):
    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        # TODO: do something smart and check whether languages are already downloaded, if so, display selection in doc
        inputs = {
            "token_lists": {
                "type": "array",
                "doc": "An array of string lists (a list of tokens).",
            },
            "languages": {
                "type": "list",
                # "doc": f"A list of language names to use default stopword lists for. Available: {', '.join(get_stopwords().fileids())}.",
                "doc": "A list of language names to use default stopword lists for.",
                "optional": True,
            },
            "additional_stopwords": {
                "type": "list",
                "doc": "A list of additional, custom stopwords.",
                "optional": True,
            },
        }
        return inputs

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:
        outputs = {
            "token_list": {
                "type": "array",
                "doc": "An array of string lists, with the stopwords removed.",
            }
        }
        return outputs

    def process(self, inputs: StepInputs, outputs: StepOutputs) -> None:

        custom_stopwords = inputs.get_value_data("additional_stopwords")
        languages = inputs.get_value_data("languages")
        if isinstance(languages, str):
            languages = [languages]

        stopwords = set()
        if languages:
            for language in languages:
                if language not in get_stopwords().fileids():
                    raise KiaraProcessingException(
                        f"Invalid language: {language}. Available: {', '.join(get_stopwords().fileids())}."
                    )
                stopwords.update(get_stopwords().words(language))

        if custom_stopwords:
            stopwords.update(custom_stopwords)

        if not stopwords:
            outputs.token_list = inputs.get_value_obj("token_lists")
            return

        token_lists = inputs.get_value_data("token_lists")

        if hasattr(token_lists, "to_pylist"):
            token_lists = token_lists.to_pylist()

        result = []
        for token_list in token_lists:

            cleaned_list = [x for x in token_list if x not in stopwords]
            result.append(cleaned_list)

        outputs.token_list = pa.array(result)


class LemmatizeModule(KiaraModule):
    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:
        inputs = {"tokens": {"type": "list", "doc": "A list of tokens."}}
        return inputs

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:
        outputs = {"tokens": {"type": "list", "doc": "A list of lemmatized tokens."}}
        return outputs

    def process(self, inputs: StepInputs, outputs: StepOutputs) -> None:

        tokens = inputs.get_value_data("tokens")
        print(f"LEMMA: {tokens[0: 20]}")

        # TODO: install this on demand?
        import it_core_news_sm

        it_nlp = it_core_news_sm.load(disable=["tagger", "parser", "ner"])

        lemmatized_doc = []
        for w in tokens:
            w_lemma = [token.lemma_ for token in it_nlp(w)]
            lemmatized_doc.append(w_lemma[0])

        outputs.tokens = lemmatized_doc
