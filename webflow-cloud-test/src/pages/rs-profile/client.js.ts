import client from '../../assets/rs-profile/client.js?raw';
import service from '../../assets/rs-profile/service.js?raw';
import css from '../../assets/rs-profile/profile.css?raw';

import recognitionCss from '../../assets/rs-recognition/widget.css?raw';
import screenCss from '../../assets/rs-ui/screen.css?raw';

export function GET(){
 const source=client.replace("import screenCss from '../rs-ui/screen.css?raw';",'const screenCss='+JSON.stringify(screenCss)+';').replace("import recognitionCss from '../rs-recognition/widget.css?raw';",'const recognitionCss='+JSON.stringify(recognitionCss)+';').replace("import profileCss from './profile.css?raw';",'const profileCss='+JSON.stringify(css)+';').replace("import {loadProfile,saveProfile,clearProfile,saveViewPreferences} from './service.js';",service.replace("import {recognitionAdapter} from '../rs-recognition/widget.js';",''));
 return new Response(source,{headers:{'Content-Type':'text/javascript; charset=utf-8','Cache-Control':'no-store'}});
}
