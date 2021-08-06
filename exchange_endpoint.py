from flask import Flask, request, g
from flask_restful import Resource, Api
from sqlalchemy import create_engine
from flask import jsonify
import json
import eth_account
import algosdk
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import load_only
from datetime import datetime
import sys

from models import Base, Order, Log
engine = create_engine('sqlite:///orders.db')
Base.metadata.bind = engine
DBSession = sessionmaker(bind=engine)

app = Flask(__name__)

@app.before_request
def create_session():
    g.session = scoped_session(DBSession)

@app.teardown_appcontext
def shutdown_session(response_or_exc):
    sys.stdout.flush()
    g.session.commit()
    g.session.remove()


""" Suggested helper methods """

def check_sig(payload,sig):
    pass

def fill_order(order):
    #Your code here
    
    # Check if there are any existing orders that match
    query = (g.session.query(Order)
              .filter(Order.filled == None)
              .filter(Order.buy_currency == order['sell_currency'])
              .filter(Order.sell_currency == order['buy_currency'])
              .filter((Order.sell_amount/Order.buy_amount) >= (order['buy_amount']/order['sell_amount']))
            )
    
    # Inserting order in database
    new_order = Order( sender_pk=order['sender_pk'],
        receiver_pk=order['receiver_pk'], 
        buy_currency=order['buy_currency'], 
        sell_currency=order['sell_currency'], 
        buy_amount=order['buy_amount'], 
        sell_amount=order['sell_amount'] )
    g.session.add(new_order)
    g.session.commit()
    
    if query.count() > 0:
        existing_order = query.first()
      
        # Set the filled field to be the current timestamp on both orders
        new_order.filled = datetime.now()
        existing_order.filled = datetime.now()
        g.session.commit()
      
        # Set counterparty_id to be the id of the other order
        new_order.counterparty_id = existing_order.id
        existing_order.counterparty_id = new_order.id
        g.session.commit()
      
        # If one of the orders is not completely filled 
        # (i.e. the counterparty’s sell_amount is less than buy_amount)
        if new_order.buy_amount < existing_order.sell_amount:
            remaining_buy = existing_order.sell_amount - new_order.buy_amount
            remaining_sell = existing_order.buy_amount - new_order.sell_amount
        
        if (remaining_buy > 0  and remaining_sell > 0 and ()):
            derived_order = Order( sender_pk=existing_order.sender_pk,
                receiver_pk=existing_order.receiver_pk, 
                buy_currency=existing_order.buy_currency, 
                sell_currency=existing_order.sell_currency, 
                buy_amount=remaining_sell, 
                sell_amount=remaining_buy,
                creator_id=existing_order.id)
            g.session.add(derived_order)
            g.session.commit()
      
        elif new_order.buy_amount > existing_order.sell_amount:
            remaining_buy = new_order.buy_amount - existing_order.sell_amount
            remaining_sell = new_order.sell_amount - existing_order.buy_amount
        
            if (remaining_buy > 0  and remaining_sell > 0):
                derived_order = Order( sender_pk=new_order.sender_pk,
                    receiver_pk=new_order.receiver_pk, 
                    buy_currency=new_order.buy_currency, 
                    sell_currency=new_order.sell_currency, 
                    buy_amount=remaining_buy, 
                    sell_amount=remaining_sell,
                    creator_id=new_order.id)
                g.session.add(derived_order)
                g.session.commit()
    pass
  
def log_message(d):
    # Takes input dictionary d and writes it to the Log table
    # Hint: use json.dumps or str() to get it in a nice string form
    # Takes input dictionary d and writes it to the Log table
    new_log = Log( message=d )

    g.session.add(new_log)
    g.session.commit()

""" End of helper methods """


@app.route('/trade', methods=['POST'])
def trade():
    print("In trade endpoint")
    if request.method == "POST":
        content = request.get_json(silent=True)
        print( f"content = {json.dumps(content)}" )
        columns = [ "sender_pk", "receiver_pk", "buy_currency", "sell_currency", "buy_amount", "sell_amount", "platform" ]
        fields = [ "sig", "payload" ]

        for field in fields:
            if not field in content.keys():
                print( f"{field} not received by Trade" )
                print( json.dumps(content) )
                log_message(content)
                return jsonify( False )
        
        for column in columns:
            if not column in content['payload'].keys():
                print( f"{column} not received by Trade" )
                print( json.dumps(content) )
                log_message(content)
                return jsonify( False )
            
    # Your code here
    # Note that you can access the database session using g.session

    # TODO: Check the signature        
    # TODO: Add the order to the database
    # TODO: Fill the order
    # TODO: Be sure to return jsonify(True) or jsonify(False) depending on if the method was successful

    result = False #Should only be true if signature validates
    sig = content['sig']
    payload = content['payload']
    payload_str = json.dumps(payload)

    if payload['platform'] == 'Ethereum':
        # Generating Ethereum account
        eth_account.Account.enable_unaudited_hdwallet_features()
        acct, mnemonic = eth_account.Account.create_with_mnemonic()
        eth_pk = acct.address
        eth_sk = acct.key

        eth_encoded_msg = eth_account.messages.encode_defunct(text=payload_str)
        if eth_account.Account.recover_message(eth_encoded_msg,signature=content['sig']) == payload['sender_pk']:
            result = True
    
    elif payload['platform']  == 'Algorand':
        print('algorand')
        if algosdk.util.verify_bytes(payload_str.encode('utf-8'),content['sig'],payload['sender_pk']):
            result = True
    
    if result == True:
        new_order = Order( sender_pk=payload['sender_pk'],
            receiver_pk=payload['receiver_pk'], 
            buy_currency=payload['buy_currency'], 
            sell_currency=payload['sell_currency'], 
            buy_amount=payload['buy_amount'], 
            sell_amount=payload['sell_amount'],
            signature=content['sig'] )
        fill_order(new_order)
        g.session.add(new_order)
        g.session.commit()
    else:
        log_message(json.dumps(payload))

    return jsonify( True )
        

@app.route('/order_book')
def order_book():
    #Your code here
    #Note that you can access the database session using g.session
    data = []
    
    query = (g.session.query(Order).all())

    for order in query:
        temp_dict = {}

        temp_dict['sender_pk'] = order.sender_pk
        temp_dict['receiver_pk'] = order.receiver_pk
        temp_dict['buy_currency'] = order.buy_currency
        temp_dict['sell_currency'] = order.sell_currency
        temp_dict['buy_amount'] = order.buy_amount
        temp_dict['sell_amount'] = order.sell_amount
        temp_dict['signature'] = order.signature

        data.append(temp_dict)
        g.session.commit()
    
    reponse = {'data': data}

    return jsonify(reponse)

if __name__ == '__main__':
    app.run(port='5002')