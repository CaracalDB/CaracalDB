import 'dart:html';
import 'dart:convert';

final String apiUrl = "http://127.0.0.1:8088/";

typedef void DataLoadHandler(String responseText);

void apiRequest(String url, DataLoadHandler handler) {
  HttpRequest.getString(apiUrl + url).then(handler);
}

ParagraphElement keyP;
ParagraphElement endKeyP;
ParagraphElement valueP;
TextInputElement keyField;
TextInputElement endKeyField;
TextInputElement valueField;
RadioButtonInputElement putRadio;
RadioButtonInputElement getRadio;
RadioButtonInputElement rangeRadio;
ParagraphElement buttonsP;
DivElement result;
Function submitHandler;

void main() {
  keyP = querySelector("#keyP");
  endKeyP = querySelector("#endKeyP");
  valueP = querySelector("#valueP");
  resetFields();
  keyField = querySelector("#keyField");
  endKeyField = querySelector("#endKeyField");
  valueField = querySelector("#valueField");
  buttonsP = querySelector("#buttons");
  result = querySelector("#result");

  putRadio = querySelector("#operation_put");
  putRadio.onClick.listen(preparePut);
  getRadio = querySelector("#operation_get");
  getRadio.onClick.listen(prepareGet);
  rangeRadio = querySelector("#operation_range");
  rangeRadio.onClick.listen(prepareRange);

  querySelector("#resetButton").onClick.listen(resetAll);
  querySelector("#submitButton").onClick.listen(runQuery);
  querySelector("#clearButton").onClick.listen(clearResults);
  clearResults(null);
}

void runQuery(MouseEvent event) {
  var result = submitHandler();

}

void displayResult(String resultText) {
  var res = new Element.p();
  res.text = resultText;
  result.insertBefore(new Element.hr(), result.childNodes.first);
  result.insertBefore(res, result.childNodes.first);
}

void preparePut(MouseEvent event) {
  resetFields();

  keyP.style.display = "block";
  valueP.style.display = "block";
  buttonsP.style.display = "block";
}

void prepareGet(MouseEvent event) {
  resetFields();
  submitHandler = () {
    String path = "schema/test/key/" + keyField.value;
    apiRequest(path, displayResult);
  };
  keyP.style.display = "block";
  buttonsP.style.display = "block";
}

void prepareRange(MouseEvent event) {
  resetFields();
  keyP.style.display = "block";
  endKeyP.style.display = "block";
  buttonsP.style.display = "block";
}

void resetAll(MouseEvent event) {
  resetFields();
  buttonsP.style.display = "none";
  keyField.value = "";
  endKeyField.value = "";
  valueField.value = "";
  putRadio.checked = false;
  getRadio.checked = false;
  rangeRadio.checked = false;
}

void resetFields() {
  keyP.style.display = "none";
  endKeyP.style.display = "none";
  valueP.style.display = "none";
}

void clearResults(MouseEvent event) {
  result.children.clear();
  result.text = "No results, yet.";
}
