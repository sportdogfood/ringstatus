import adapter from '../../assets/rs-recognition/widget.js?raw';
import view from '../../assets/rs-recognition/widget-view.js?raw';
import css from '../../assets/rs-recognition/widget.css?raw';

export function GET(){
 const source=adapter.replace("import {mountRecognize} from './widget-view.js';",view.replace("import widgetCss from './widget.css?raw';",'const widgetCss='+JSON.stringify(css)+';'));
 return new Response(source,{headers:{'Content-Type':'text/javascript; charset=utf-8','Cache-Control':'no-store'}});
}
