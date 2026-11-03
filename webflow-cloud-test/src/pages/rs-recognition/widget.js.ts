import adapter from '../../assets/rs-recognition/widget.js?raw';
import view from '../../assets/rs-recognition/widget-view.js?raw';
import css from '../../assets/rs-recognition/widget.css?raw';
import screenCss from '../../assets/rs-ui/screen.css?raw';

export function GET(){
 const source=adapter.replace("import {mountRecognize} from './widget-view.js';",view.replace("import screenCss from '../rs-ui/screen.css?raw';",'const screenCss='+JSON.stringify(screenCss)+';').replace("import widgetCss from './widget.css?raw';",'const widgetCss='+JSON.stringify(css)+';'));
 return new Response(source,{headers:{'Content-Type':'text/javascript; charset=utf-8','Cache-Control':'no-store'}});
}
