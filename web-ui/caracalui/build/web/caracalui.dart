import 'dart:html';
import 'dart:convert';
import 'package:dart_config/config.dart';
import 'package:dart_config/default_browser.dart';

String apiUrl;

typedef void DataLoadHandler(String responseText);
typedef void ResponseHandler(HttpRequest response);

void apiRequest(String url, DataLoadHandler handler) {
  HttpRequest.getString(apiUrl + url).then(handler);
}

void apiMethodRequest(String url, String type, String data, ResponseHandler handler) {
  HttpRequest.request(apiUrl + url, method: type, sendData: data).then(handler);
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
ParagraphElement noResultsP;
DivElement result;
Function submitHandler;

void main() {
  loadConfig().then((Map config) {
    apiUrl = config["apiUrl"];
    // continue with your app here!
  
  keyP = querySelector("#keyP");
  endKeyP = querySelector("#endKeyP");
  valueP = querySelector("#valueP");
  resetFields();
  keyField = querySelector("#keyField");
  endKeyField = querySelector("#endKeyField");
  valueField = querySelector("#valueField");
  buttonsP = querySelector("#buttons");
  noResultsP = querySelector("#noresults");
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
  }, onError: (error) => print(error));
}

void runQuery(MouseEvent event) {
  var result = submitHandler();

}

void displayResult(String resultText) {
  var res = new Element.p();
  res.text = resultText;
  result.insertBefore(new Element.hr(), result.childNodes.first);
  result.insertBefore(res, result.childNodes.first);
  noResultsP.style.display = "none";
}

void displayHttpResult(HttpRequest resultRequest) {
  displayResult(resultRequest.responseText);
}

void preparePut(MouseEvent event) {
  resetFields();
  submitHandler = () {
    String path = "schema/test/key/" + keyField.value;
    apiMethodRequest(path, "POST", valueField.value, displayHttpResult);
  };
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
  submitHandler = () {
    if (keyField.value == "") {
      String path = "schema/test";
      apiRequest(path, displayResult);
    } else if (endKeyField.value == "") {
      String path = "schema/test/prefix/" + keyField.value;
      apiRequest(path, displayResult);
    } else {
      String path = "schema/test/range/" + keyField.value + "/" + endKeyField.value;
      apiRequest(path, displayResult);
    }
  };
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
  result.children.add(noResultsP);
  noResultsP.style.display = "block";
}
