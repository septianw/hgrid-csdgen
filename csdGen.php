<?php
// class CsdGen

class CsdGen {
  private $xml;
  private $elemHgrid247;
  private $wf;
  private $base;
  private $flowProperty = array(
    "ReducerNumber" => "",
    "MapNumber" => "",
    "actionInputDir" => "L",
    "compresstype" => "DefaultCodec",
    "jobTrackerUrl" => "localhost:50030",
    "graphDisplay" => "false",
    "mapOutputCompressed" => "false",
    "description" => "",
    "logPath" => "",
    "speculativeExecution" => "",
    "ingoreUnexistsPath" => "No",
    "combinedInput" => "false",
    "maxSplitSize" => 128,
    "fileNdelimiter" => "No,|",
    "reuseJVM" => "false",
    "zoom" => 1,
    "jobConf" => "",
    "logLoc" => "Local",
  );

  private $hfsSource = array(
    "fields" => "",
    "path" => "",
    "sinkMode" => "",
    "label" => "HFS_Source",    // editable with default
    "hfsName" => "auto int name with id",    // generated
    "ID" => "auto int",  // generated
    "type" => "HFSSOURCE",
    "posx" => 100,  // calculated with default
    "posy" => 100,  // calculated with default
    "inputdir" => "",
    "outputdir" => "",
    "testdir" => "",
    "tableHeader" => "",
    "outputWithDate" => "false",
    "headerDelimited" => "",
    "outputFormat" => "hgrid247-#####", // configurable
    "fileType" => "Hfs",
    "fieldNames" => "",
    "enabled" => "true",
    "enabledCounter" => "true",
    "argumentOrder" => "0", // configurable
    "description" => "",
    "schemeClass" => "",
    "delimiter" => "",
    "fieldGroupPartition" => "",
    "incomingFields" => "",
    "partDelimiter" => "",
    "partIndex" => "",
    "rejectedHfs" => "false",
    "sourceFields" => "",
    "sinkFields" => "",
    "numSinkPart" => "",
    "combined" => "false",
    "includeFile" => "false",
  );

  private $hfsSink = array(
    "fields" => "",
    "path" => "",
    "sinkMode" => "SinkMode.REPLACE",
    "label" => "",
    "hfsName" => "",
    "ID" => "",
    "type" => "HFSSINK",
    "posx" => 100,
    "posy" => 100,
    "inputdir" => "",
    "outputdir" => "",
    "testdir" => "",
    "tableHeader" => "",
    "outputWithDate" => "",
    "headerDelimited" => "",
    "outputFormat" => "",
    "fileType" => "",
    "fieldNames" => "",
    "enabled" => "true",
    "enabledCounter" => "",
    "argumentOrder" => "",
    "description" => "",
    "schemeClass" => "",
    "delimiter" => "",
    "fieldGroupPartition" => "",
    "incomingFields" => "",
    "partDelimiter" => "",
    "partIndex" => "",
    "rejectedHfs" => "false",
    "cachedHfs" => "false",
    "sourceFields" => "",
    "sinkFields" => "",
    "numSinkPart" => ""
  );

  private $eachSource = array(
    "name" => "",
    "ID" => "",    // generated
    "label" => "Transformator",    // editable with default
    "prevPipe" => "",    // generated
    "type" => "EACH",
    "pipeName" => "",
    "maxPrevNode" => 0,
    "flowName" => "",
    "inFields" => "",
    "outFields" => "",
    "inFieldsType" => "",
    "outFieldsType" => "",
    "posx" => 100,    // generated
    "posy" => 100,    // generated
    "tinggi" => 42,
    "lebar" => 42,
    "enabled" => "true",
    "enabledCounter" => "true",
    "description" => "",
    "argumentSelector" => "",
    "outputSelector" => "",
    "fieldDeclaration" => "",
    "posxIR" => 0,
    "posyIR" => 0,
    "posxOR" => 0,
    "posyOR" => 0,
    "widhtIR" => 0,
    "heightIR" => 0,
    "widthOR" => 0,
    "heightOR" => 0,
    "filterInstance" => "",
    "chaining" => "false"
  );

  private $transformSource = array(
    "parentID" => "",
    "prevIDS" => "",
    "nextIDS" => "",
    "ID" => "",
    "name" => "",
    "label" => "",
    "input" => "",
    "output" => "",
    "parameter" => "",
    "posx" => "",
    "posy" => "",
    "inputCount" => "",
    "outputCount" => "",
    "codeString" => "",
    "expressionString" => "",
    "defaultString" => "",
    "extraInfo" => "1isnn_sign",
    "udf" => "false",
    "edited" => "false",
  );

  private function appendChildArray(&$elem, $childArray) {
    $xml =& $this->xml;
    foreach($childArray as $k => $v) {
      $t = $xml->createElement($k, " ".(string)$v);
      $elem->appendChild($t);
      unset($t);
    }
  }

  private static function addField(&$sourceConfig, $config) {
    if (isset($config['fieldIn'])) {
      $sourceConfig['inFields'] = implode(",", array_keys($config['fieldIn']));
      $sourceConfig['inFieldsType'] = implode(",", array_values($config['fieldIn']));
    }

    if (isset($config['fieldOut'])) {
      $sourceConfig['outFields'] = implode(",", array_keys($config['fieldOut']));
      $sourceConfig['outFieldsType'] = implode(",", array_values($config['fieldOut']));
    }
  }

  private function newEmpty() {
    $self =& $this;
    $xml =& $self->xml;
    $elemHgrid247 =& $self->elemHgrid247;
    $xml = new DOMDocument("1.0");

    $elemHgrid247 = $xml->createElement('HGrid247');
    $attrHgrid247 = $xml->createAttribute('version');
    $attrHgrid247->value = '2.3.5';
    $elemHgrid247->appendChild($attrHgrid247);

    $elemFlowProperty = $xml->createElement('FlowProperty');
    $this->appendChildArray($elemFlowProperty, $this->flowProperty);
    $elemHgrid247->appendChild($elemFlowProperty);

    $xml->appendChild($elemHgrid247);

    return $self;
  }

  private function pipe($node1, $node2) {
    $self =& $this;
    $xml =& $self->xml;
    $elemHgrid247 =& $self->elemHgrid247;

    $fromid = $node1['data']['ID'];  // from current id
    $toid = $node2['data']['ID'];  // to next id

    $fromtype = $node1['data']['type'];  // from current type
    $totype = $node2['data']['type'];  // to next type

    $pipeType = "";

    switch($fromtype) {
      case "HFSSOURCE":
        $pipeType .= "Hfs2";
        break;
      case "EACH":
        $pipeType .= "Pipe2";
        break;
    }

    switch ($totype) {
      case "HFSSOURCE":
        $pipeType .= "Hfs";
        break;
      case "EACH":
        $pipeType .= "Pipe";
        break;
      case "HFSSINK":
        $pipeType .= "Hfs";
        break;
    }

    $pipe = $xml->createElement($pipeType);
    $this->appendChildArray($pipe, array('fromID' => $fromid, 'toID' => $toid));
    $elemHgrid247->appendChild($pipe);
    $xml->appendChild($elemHgrid247);
  }

  private function RegexSplitter($splitter, $acceptUnmatched) {
    $out = "";
    switch ($splitter) {
      case "|":
        $out .= "pip_sign";
        break;
    }
    $out .= ((bool)$acceptUnmatched) ? ",true" : ",false";

    return $out;
  }

  private function CurrentTimestampString($param) {
    return str_replace('-', 'min_sign', $param);
  }

  private function CurrentTimestamp($param) {
    // return str_replace('-', 'min_sign', $param);
  }

  private function TimestampToString($param) {
    // return str_replace('-', 'min_sign', $param);
  }

  private function StringToTimestamp($param) {
    return str_replace('-', 'min_sign', $param);
  }

  private function AddDay($param) {
    return $param;
  }

  /**
   * FieldJoiner
   *
   * Function to generate parameter of HGrid FieldJoiner
   * @param  string $param Input parameter of FieldJoiner
   * @return string        HGrid FieldJoiner parameter
   */
  private function FieldJoiner($param) {
    return str_replace('|', 'pip_sign', $param);
  }

  /**
   * SwitchCase
   *
   * Function to generate parameter of HGrid SwitchCase
   * @param  string $param Input parameter of SwitchCase
   * @return string        HGrid SwitchCase parameter
   */
  private function SwitchCase($param) {
    return $param;
  }

  /**
   * identity
   *
   * Identity array of field to another array of field.
   * @param  array $in  Array of first fields
   * @param  array $out Array of second fields
   * @return array      Array key => value of identitied fields.
   */
  public static function identity($in, $out) {
    $result = array();
    foreach ($in as $k => $v) {
      $result[$v] = $out[$k];
    }
    return $result;
  }

  /**
   * inherit
   *
   * Inherit is a function to inherit field from previous node to current node.
   * @param  object $p Previous node object
   * @param  object $v Reference of current node values.
   * @param  object $c Reference of current node.
   * @return void      This function does not return anything
   */
  private function inherit($p, &$v, &$c) {
    $t = array();
    if (isset($v['data']['inFields']) && isset($v['data']['inFieldsType'])) {
      $t['inFields'] = implode(",", array_merge(explode(',',$p['data']['outFields']), explode(',',$v['data']['inFields'])));
      $t['inFieldsType'] = implode(",", array_merge(explode(',',$p['data']['outFieldsType']), explode(',',$v['data']['inFieldsType'])));
    } else {
      $t['inFields'] = $p['data']['outFields'];
      $t['inFieldsType'] = $p['data']['outFieldsType'];
    }

    if (isset($v['data']['outFields']) && isset($v['data']['outFieldsType'])) {
      $t['outFields'] = implode(",", array_merge(explode(',',$p['data']['outFields']), explode(',',$v['data']['outFields'])));
      $t['outFieldsType'] = implode(",", array_merge(explode(',',$p['data']['outFieldsType']), explode(',',$v['data']['outFieldsType'])));
    } else {
      $t['outFields'] = $p['data']['outFields'];
      $t['outFieldsType'] = $p['data']['outFieldsType'];
    }
    switch($v['data']['inherit']) {
      case 11: // all
        $v['data']['inFields'] = $t['inFields'];
        $v['data']['inFieldsType'] = $t['inFieldsType'];
        $v['data']['outFields'] = $t['outFields'];
        $v['data']['outFieldsType'] = $t['outFieldsType'];

        $c['data']['inFields'] = $t['inFields'];
        $c['data']['inFieldsType'] = $t['inFieldsType'];
        $c['data']['outFields'] = $t['outFields'];
        $c['data']['outFieldsType'] = $t['outFieldsType'];
        unset($v['data']['inherit']);
        break;
      case 10: // input
        $v['data']['inFields'] = $t['inFields'];
        $v['data']['inFieldsType'] = $t['inFieldsType'];

        $c['data']['inFields'] = $t['inFields'];
        $c['data']['inFieldsType'] = $t['inFieldsType'];
        unset($v['data']['inherit']);
        break;
      case 01: // output
        $v['data']['outFields'] = $t['outFields'];
        $v['data']['outFieldsType'] = $t['outFieldsType'];

        $c['data']['outFields'] = $t['outFields'];
        $c['data']['outFieldsType'] = $t['outFieldsType'];
        unset($v['data']['inherit']);
        break;
    }
  }

  /**
   * newTransformator
   *
   * Generate new transformator.
   * Transformator is an object inside a node. Transformator can be anything
   * inside a transformator node. Including switch case, field split,
   * field joiner, and anything supported by HGrid.
   * @param  mixed $parentID  id of parent node
   * @param  object $config   configuration object of transformator
   * @return object           Final chain of transformator config
   */
  private function newTransformator($parentID, $config) {
    $self =& $this;
    $xml =& $self->xml;
    $transformators =& $config;
    $out = array();
    $c = 0;

    foreach ($transformators as $k => $v) {
      $now = uniqid();
      $transformators[$k]['ID'] = $now;
    }
    foreach ($transformators as $k => $v) {
      $sourceConfig = $self->transformSource;
      $sourceConfig['parentID'] = $parentID;
      $sourceConfig['ID'] = $v['ID'];

      $sourceConfig['name'] = array_keys($v['process'])[0];
      $sourceConfig['label'] = sprintf("%s(%s)", array_keys($v['process'])[0], array_values($v['process'])[0]);
      if (isset($v['in'])) {
        $sourceConfig['input'] = implode(',', array_keys($v['in']));
      }
      if (isset($v['out'])) {
        $sourceConfig['output'] = implode(',', array_keys($v['out']));// echo $v['process'][0]; echo gettype($v['process'][0]); die();
      }
      $t = key($v['process']);
      $sourceConfig['parameter'] = call_user_func_array(array($self, $t), explode(',', $v['process'][$t]));
      $sourceConfig['posx'] = 250 + ($c * 100);
      $sourceConfig['posy'] = 140;
      if (isset($v['in'])) {
        $sourceConfig['inputCount'] = count($v['in']);
      }
      if (isset($v['out'])) {
        $sourceConfig['outputCount'] = count($v['out']);
      }

      if (isset($v['from'])) {
        $sourceConfig['prevIDS'] = $transformators[$v['from']]['ID'];
      }
      if (isset($v['to'])) {
        $sourceConfig['nextIDS'] = $transformators[$v['to']]['ID'];
      }

      // Masukkan yang lain yang belum masuk, dan langsung masuk tanpa validasi
      foreach($v as $ke => $ve) {
        $sourceConfig[$ke] = $v[$ke];
      }
      // Keluarkan yang sebelumnya sudah masuk lewat validasi
      foreach(array('in', 'out', 'process', 'from', 'to') as $ve) {
        unset($sourceConfig[$ve]);
      }

      $out[] = $sourceConfig;
      $c++;
    }


    return $out;
  }

  /**
   * Class constructor
   */
  function __construct() {
    $this->base = $this->newEmpty();
  }

  /**
   * getFlowProperty
   *
   * Get private property of flow
   * @return object Flow property object
   */
  function getFlowProperty() {
    return $this->flowProperty;
  }

  /**
   * setFlowProperty
   *
   * Set private property of flow
   * @param object $flowProperty Flow property object
   */
  function setFlowProperty($flowProperty) {
    $this->flowProperty = $flowProperty;
  }

  /**
   * newHfsSource
   *
   * Generate new source node
   * @param  object $config Configuration object
   * @return object         Current object
   */
  function newHfsSource($config) {
    $self =& $this;
    $xml =& $self->xml;

    $sourceConfig = $self->hfsSource;
    $now = uniqid();

    $sourceConfig['label'] = ($config['label'] !== null) ? $config['label'] : "HFS_Source";
    $sourceConfig['ID'] = $now;
    $sourceConfig['hfsName'] = str_replace(" ", "_", $sourceConfig['label']) . "_" . $now;
    $sourceConfig['path'] = sprintf('input_%s_%s', $sourceConfig['label'], $now);

    $node['type'] = 'HfsSource';
    $node['data'] = $sourceConfig;

    return $node;
  }

  /**
   * newHfsSink
   *
   * Generate new sink node.
   * @param  object $config Configuration object
   * @return object         current object
   */
  function newHfsSink($config) {
    $self =& $this;
    $xml =& $self->xml;

    $sourceConfig = $self->hfsSink;
    $now = uniqid();

    $sourceConfig['label'] = ($config['label'] !== null) ? $config['label'] : "HFS_Sink";
    $sourceConfig['ID'] = $now;
    $sourceConfig['hfsName'] = str_replace(" ", "_", $sourceConfig['label']) . "_" . $now;
    $sourceConfig['path'] = sprintf('output_%s_%s', $sourceConfig['label'], $now);

    $node['type'] = 'HfsSink';
    $node['data'] = $sourceConfig;

    return $node;
  }

  /**
   * newEach
   *
   * Generate new transformator
   * @param  object $config Configuration object
   * @return object         current object
   */
  function newEach($config) {
    $self =& $this;
    $xml =& $self->xml;
    $sourceConfig = $self->eachSource;
    $now = uniqid();
    $sourceConfig['label'] = ($config['label'] !== null) ? $config['label'] : "Transformator";
    $sourceConfig['ID'] = $now;
    $sourceConfig['pipeName'] = str_replace(" ", "_", $sourceConfig['label']) . "_" . $now;

    if (isset($config['inherit'])) {
      $sourceConfig['inherit'] = $config['inherit'];
      if (isset($config['fieldIn']) || isset($config['fieldOut'])) {
        $self::addField($sourceConfig, $config);
        unset($config['fieldIn'], $config['fieldOut']);
      }
    } else {
      $self::addField($sourceConfig, $config);
      unset($config['fieldIn'], $config['fieldOut']);
    }

    if (isset($config['transformators'])) {
      $transformator = $config['transformators'];

      $sourceConfig['transformators']= $this->newTransformator($sourceConfig['ID'], $transformator);
      unset($config['transformators']);
    }


    if (isset($config['identity'])) {
      $sourceConfig['identity'] = $config['identity'];
    }

    $node['type'] = 'Each';
    $node['data'] = $sourceConfig;

    return $node;
  }

  /**
   * startFrom
   *
   * startFrom act as initiator for node chain.
   * @param  object $node node that genereratable by function prefixed with new
   * @return object       this object
   */
  function startFrom($node) {
    $self =& $this;
    $xml =& $self->xml;

    $self->wf[] = $node;
    return $self;
  }

  /**
   * pointTo
   *
   * Point a node to another node.
   * @param  object $node node that generatable by function prefixed with new
   * @return object       current object
   */
  function pointTo($node) {
    return $this->startFrom($node);
  }

  // Semua dikirim kesini untuk compile XML
  /**
   * compile
   *
   * Compile chainable object into whole object that can be generated to XML
   * @return object Current class
   */
  function compile() {
    $self =& $this;
    $xml =& $self->xml;

    $elemHgrid247 = $self->elemHgrid247;

    foreach($self->wf as $k => $v) {
      // alur maju
      if (count($self->wf) - 1 == $k) { // terakhir
      } else { // belum berakhir
        $self->pipe($v, $self->wf[$k+1]);
      }

      // alur mundur
      if ($k == 0) {  // pertama
      } else {  // kedua dan seterusnya
        $p = $self->wf[$k-1]; // previous node
        $c =& $self->wf[$k];  // current node

        $v['data']['posx'] = $p['data']['posx'] + 100;
        $self->wf[$k]['data']['posx'] = $v['data']['posx'];

        if (isset($v['data']['inherit']) && $v['data']['inherit'] != 0) {
          $self->inherit($p, $v, $c); // Previous, Current value, Current node.
        }
      }

      // bila ada transformator
      if (isset($v['data']['transformators'])) {
        $transformators = $v['data']['transformators'];
        foreach($transformators as $ke => $ve) {
          $t = $xml->createElement('Transform');
          $self->appendChildArray($t, $ve);
          $elemHgrid247->appendChild($t);
          unset($t);
        }
        unset($v['data']['transformators']);
      }


      // bila ada identity
      if (isset($v['data']['identity'])) {
        foreach($v['data']['identity'] as $ka => $va) {
          $t = $xml->createElement('Identity');
          $self->appendChildArray($t, array(
            'parentID' => $v['data']['ID'],
            'fromField' => $ka,
            'toField' => $va
          ));
          $elemHgrid247->appendChild($t);
        }
        unset($v['data']['identity']);
      }

      $node = $xml->createElement($v['type']);
      $self->appendChildArray($node, $v['data']);
      $elemHgrid247->appendChild($node);
      unset($node);
    }

    return $this;
  }

  /**
   * stringXML
   *
   * return CSD XML result
   * @return string Generated CSD XML
   */
  function stringXML() {
    $xml =& $this->xml;
    $xml->formatOutput = true;
    return $xml->saveXML();
  }
}
