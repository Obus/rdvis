__author__ = 'senov'
import SocketServer
import SimpleHTTPServer
import urlparse
import impala_wrapper
import urllib
import urlparse

_PORT = 8000


class MyHttpRequestHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):
    def end_headers(self):
        self.send_my_headers()

        SimpleHTTPServer.SimpleHTTPRequestHandler.end_headers(self)

    def send_my_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")

    def do_HEAD(self):
        SimpleHTTPServer.SimpleHTTPRequestHandler.do_HEAD(self)

    def do_GET(self):
        print self.path
        raw_param_dict = urlparse.parse_qs(urlparse.urlparse(self.path).query)
        param_dict = {}
        for k, v in raw_param_dict.items():
            param_dict[k] = v[0]

        if self.path.startswith("/consumer_recs?"):
            self.wfile.write(impala_wrapper.get_consumer_basket(discount_card_id=param_dict['discount_card_id']))
        elif self.path.startswith("/consumer_date_purchases?"):
            self.wfile.write(impala_wrapper.get_consumer_cheques(discount_card_id=param_dict['discount_card_id']))
        elif self.path.startswith("/consumer_date_group_purchases?"):
            self.wfile.write(impala_wrapper.get_consumer_group_cheques(discount_card_id=param_dict['discount_card_id']))
        elif self.path.startswith("/histogram?"):
            if 'series_column_name' in param_dict:
                self.wfile.write(
                    impala_wrapper.get_histogram_stacked(
                        table_name=param_dict['table_name'],
                        x_axis_column_name=param_dict['x_axis_column_name'],
                        y_axis_column_name=param_dict['y_axis_column_name'],
                        series_column_name=param_dict['series_column_name']
                    )
                )
            else:
                self.wfile.write(
                    impala_wrapper.get_histogram_single(
                        table_name=param_dict['table_name'],
                        x_axis_column_name=param_dict['x_axis_column_name'],
                        y_axis_column_name=param_dict['y_axis_column_name']
                    )
                )
        elif self.path.startswith("/hard_query?"):
            # params_string = self.path.replace("/hard_query?", "")
            # param_dict = http_params_to_dict(params_string)
            # query = params_string.replace('query=', '')
            query = param_dict['query']
            query = urllib.unquote(query)
            query = query[1:-1]
            self.wfile.write(impala_wrapper.get_consumer_basket(query=query))
        else:
            SimpleHTTPServer.SimpleHTTPRequestHandler.do_GET(self)


def http_params_to_dict(params_string):
    param_pairs = params_string.split('&')
    d = dict()
    for pp in param_pairs:
        n, v = pp.split('=')
        d[n] = v
    return d


def http_params_to_dict(params_string):
    param_pairs = params_string.split('&')
    d = dict()
    for pp in param_pairs:
        n, v = pp.split('=')
        d[n] = v
    return d


httpd = SocketServer.ForkingTCPServer(('', _PORT), MyHttpRequestHandler)
print "serving at port", _PORT
httpd.serve_forever()
