<?php
namespace Zelenka {
  class Event {
    $connected    = false;
    $records      = Array();
    $error        = '';

    function __destruct() {
      $fp = fsockopen("localhost", 9888, $errno, $errstr, 30);
      if (!$fp) {
        $this->error = "$errstr ($errno)<br />\n";
      } else {
        for ($this->$records as $record)
          fwrite($fp, json_encode($this->records)."\n");
        fclose($fp);
      }
    }

    function record(data) {
      $this->records[] = $data;
    }
  }
}