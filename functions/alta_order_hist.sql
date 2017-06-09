CREATE OR REPLACE FUNCTION fenamosq.alta_order_hist(
    IN pi_id_order numeric,
    IN pi_master character varying,
    IN pi_tran_date timestamp without time zone,
    IN pi_tran_id numeric,
    IN pi_tran_user character varying,
    OUT po_id_order numeric,
    OUT po_tran_id numeric,
    OUT po_cod_err character varying,
    OUT po_msj_err character varying,
    OUT po_msj_err_pl character varying)
  RETURNS record AS
$BODY$
  DECLARE
      c_cod_transaction  varchar(200):='ALTA_ORD_ORDER';
      v_tran_id          NUMERIC;
      v_tran_user        VARCHAR(50);
      v_tran_date        TIMESTAMP;
      c_falso            varchar(2):='0';  
      v_aplica           varchar(2):='1';
      c_aplica           varchar(2):='1';
      v_id_task          numeric;
      task_record        record;
      task_js            json;
      v_descrip          varchar(200);
      v_id_order   numeric;

  BEGIN 
       
      --Obtener valores transacccionales
      IF pi_master = c_aplica THEN 
          
          SELECT rs.po_tran_id,
                 rs.po_tran_date,
                 rs.po_cod_err,
                 rs.po_msj_err,
                 rs.po_msj_err_pl
           INTO  v_tran_id,
                 v_tran_date,
                 po_cod_err,
                 po_msj_err,
                 po_msj_err_pl
           FROM fenamosq.get_transaction(
                pi_cod_transaction    => c_cod_transaction,
                pi_tran_user          => pi_tran_user
           ) rs;

          IF po_cod_err <> 'OK' THEN 
              RAISE EXCEPTION 'TRANSFER';
          END IF;

      ELSIF pi_tran_date IS NULL OR  pi_tran_id IS NULL  OR pi_tran_user  IS NULL THEN
          po_cod_err := 'ALTA-ORDER-001';
          RAISE EXCEPTION 'CONDICTION';
      ELSE 
          v_tran_id := pi_tran_id;
          v_tran_date  := pi_tran_date;
      END IF; 

      SELECT rs.po_aplica 
        INTO v_aplica 
        FROM fenamosq.validate_transaction(
              pi_cod_transaction => c_cod_transaction,
              pi_tran_user        => pi_tran_user
            ) rs;

      IF v_aplica = '0' THEN 
          po_cod_err := 'ALTA-ORDER-002';
          RAISE EXCEPTION 'CONDICTION';
      ELSIF v_aplica = '2' THEN 
          po_cod_err := 'ALTA-ORDER-003';
          RAISE EXCEPTION 'CONDICTION';
      END IF;

      po_cod_err := 'ALTA-ORDER-004';
      
      po_cod_err := 'ALTA-ORDER-006';

      
      INSERT INTO fenamosq.ord_order_hist(
            id_order, ord_orden_seq, id_client, date_created, cod_priority, description, 
            observation, order_state, quant_order, state, tran_date, tran_id, tran_user)
       VALUES (pi_id_order, v_id_order, pi_id_cliente, now(), 'STANDAR', pi_description, pi_observation,  
            'CREATED', pi_quantity, 'A', v_tran_date, v_tran_id, pi_tran_user);

      
      po_cod_err := 'OK';
      po_tran_id := v_tran_id;
      po_id_order := v_id_order;
  EXCEPTION
   WHEN OTHERS THEN 
        po_tran_id := v_tran_id;

        IF(SQLERRM <> 'TRANSFER') THEN 
            po_msj_err_pl = SQLERRM;
        END IF;

        IF pi_master = c_aplica THEN
          
          SELECT rs.po_msj_err
            into po_msj_err 
            FROM fenamosq.set_transaction_log(
                pi_tran_id         => v_tran_id,
                pi_tran_cod        => c_cod_transaction,
                pi_tran_date       => v_tran_date,    
                pi_tran_user       => pi_tran_user,
                pi_cod_err         => po_cod_err,
                pi_msj_err_pl      => po_msj_err_pl) rs;
        END IF;


  END; 
  $BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION fenamosq.alta_order(numeric, character varying, character varying, numeric, character varying, character varying, timestamp without time zone, numeric, character varying)
  OWNER TO fenamo;
